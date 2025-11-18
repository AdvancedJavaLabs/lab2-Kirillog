import com.vader.sentiment.analyzer.SentimentAnalyzer
import dev.kourier.amqp.connection.amqpConfig
import dev.kourier.amqp.connection.createAMQPConnection
import dev.kourier.amqp.properties
import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import opennlp.tools.namefind.NameFinderME
import opennlp.tools.namefind.TokenNameFinderModel
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import kotlin.system.measureTimeMillis


val config = amqpConfig {
    server {
        host = "localhost"
    }
}
const val topNCount = 5

@Serializable
data class Request(
    val id: Int,
    val section: String,
)

@Serializable
data class Polarity(
    val positivePolarity: Float,
    val negativePolarity: Float,
    val neutralPolarity: Float,
    val compoundPolarity: Float
) {

    operator fun plus(other: Polarity): Polarity {
        return Polarity(
            (positivePolarity + other.positivePolarity) / 2f,
            (negativePolarity + other.negativePolarity) / 2f,
            (neutralPolarity + other.neutralPolarity) / 2f,
            (compoundPolarity + other.compoundPolarity) / 2f
        )
    }
}

@Serializable
data class Result(
    val id: Int,
    val count: Int,
    val mostFrequent: List<Pair<String, Int>>,
    val polarity: Polarity,
    val replacedNames: List<String>,
    val sortedByLength: List<String>
) {
    operator fun plus(other: Result): Result {
        val most2Frequent = (mostFrequent + other.mostFrequent).groupBy { it.first }
            .map { Pair(it.key, it.value.sumOf { it.second }) }.sortedByDescending { it.second }.take(topNCount)
//        println(this)
        return Result(
            id + 1,
            count + other.count,
            most2Frequent,
            polarity + other.polarity,
            (replacedNames + other.replacedNames).distinct(),
            sortedByLength + other.sortedByLength
        )
    }
}

suspend fun producer(coroutineScope: CoroutineScope, filename: String) {

    val connection = createAMQPConnection(coroutineScope, config)
    val channel = connection.openChannel()

    channel.queueDeclare(
        "task_queue",
        durable = false,
        exclusive = false,
        autoDelete = true,
        arguments = emptyMap()
    )

    channel.queueDeclare(
        "task_count_queue",
        durable = false,
        exclusive = false,
        autoDelete = true,
        arguments = emptyMap()
    )

    val properties = properties {
        deliveryMode = 2u
    }

    val file = File("src/main/resources/corpus/${filename}.txt")

    val iter = file.readText().split("\n\n").withIndex()
    val chunkCount = iter.count()

    channel.basicPublish(
        "$chunkCount".toByteArray(),
        exchange = "",
        routingKey = "task_count_queue",
        properties = properties
    )

//    println(" [Producer] Sent size: '$chunkCount'");

    for ((id, chunk) in iter) {
        val request = Request(id, chunk)
        val serializedMessage = Json.encodeToString(request)
        channel.basicPublish(
            serializedMessage.toByteArray(),
            exchange = "",
            routingKey = "task_queue",
            properties = properties
        )
//        println(" [x] Sent '$chunk'")
    }

    channel.close()
    connection.close()
}

suspend fun worker(coroutineScope: CoroutineScope, nameFinderME: TokenNameFinderModel, workerName: String) {
    val connection = createAMQPConnection(coroutineScope, config)
    val channel = connection.openChannel()

    channel.queueDeclare(
        "task_queue",
        durable = false,
        exclusive = false,
        autoDelete = true,
        arguments = emptyMap()
    )
    channel.queueDeclare(
        "result_queue",
        durable = false,
        exclusive = false,
        autoDelete = true,
        arguments = emptyMap()
    )

//    println(" [$workerName] Waiting for messages.")

    channel.basicQos(count = 1u, global = false)

    val consumer = channel.basicConsume("task_queue", noAck = false)

    for (delivery in consumer) {
        val request: Request = Json.decodeFromString(delivery.message.body.decodeToString())
//        println(" [$workerName] Received '${request.id}'")

        try {
            val result = processSection(nameFinderME, request)
            val serializedRequest = Json.encodeToString(result)
//            println(" [$workerName] Done")
            channel.basicPublish(
                serializedRequest.toByteArray(),
                exchange = "",
                routingKey = "result_queue"
            )
        } finally {
            channel.basicAck(delivery.message, multiple = false)
        }
    }

    channel.close()
    connection.close()
}

suspend fun aggregator(coroutineScope: CoroutineScope) {
    val connection = createAMQPConnection(coroutineScope, config)
    val channel = connection.openChannel()
    channel.queueDeclare(
        "result_queue",
        durable = false,
        exclusive = false,
        autoDelete = true,
        arguments = emptyMap()
    )

    channel.queueDeclare(
        "task_count_queue",
        durable = false,
        exclusive = false,
        autoDelete = true,
        arguments = emptyMap()
    )

    val countConsumer = channel.basicConsume("task_count_queue", noAck = false)
    val countDelivery = countConsumer.receive()
    val count =
        try {
            countDelivery.message.body.decodeToString().toInt()
        } finally {
            channel.basicAck(countDelivery.message, multiple = false)
        }
//    println(" [Aggregator]: Got count $count")

    val consumer = channel.basicConsume("result_queue", noAck = false)

    var result = Result(0, 0, listOf(), Polarity(0f, 0f, 1f, 0f), listOf(), listOf())
    for (delivery in consumer) {
        try {
            val req: Result = Json.decodeFromString(delivery.message.body.decodeToString())
//            println(" [Aggregator]: Receive ${result.id} / $count")
            result += req
        } finally {
            channel.basicAck(delivery.message, multiple = false)
        }
        if (result.id == count) {
            break
        }
    }

    withContext(Dispatchers.IO) {
        FileOutputStream("result.json").use {
            it.writer(Charsets.UTF_8).use {
                it.append(Json.encodeToString(result))
            }
        }
    }

//    println(" [Aggregator]: Dumped result to file")

    channel.close()
    connection.close()
}

private fun processSection(nameFinder: TokenNameFinderModel, req: Request): Result {
    val nameFinderME = NameFinderME(nameFinder)
    val sentences = req.section.split(".", "?", "!").map { it.trim() }
    val words = req.section.split("\\s+".toRegex())
    val names = mutableSetOf<String>()
    val topN =
        words.groupBy { it }.map { Pair(it.key, it.value.size) }.sortedByDescending { it.second }.take(topNCount)
    val sentimentPolarities = SentimentAnalyzer.getScoresFor(req.section)
    for (span in nameFinderME.find(words.toTypedArray())) {
        names.addAll(words.subList(span.start, span.end))
    }
    val sortedSentences = sentences.sortedBy { it.length }
    return Result(
        req.id,
        words.count(),
        topN,
        Polarity(
            sentimentPolarities.positivePolarity,
            sentimentPolarities.negativePolarity,
            sentimentPolarities.neutralPolarity,
            sentimentPolarities.compoundPolarity
        ),
        names.toList(),
        sortedSentences
    )
}


fun main(args: Array<String>): Unit = runBlocking {

    val fileName = if (args.isEmpty()) "sample" else args[0]
    val workerCount = if (args.size < 2) 4 else args[1].toInt()

    val model = FileInputStream("src/main/resources/en-ner-person.bin").use { modelIn ->
        TokenNameFinderModel(modelIn)
    }
    run(fileName, workerCount, model)
}

private suspend fun measure(model: TokenNameFinderModel) {
    for (fileName in listOf("sample", "Lee_Albany_1884", "Woolf_Years_1937", "Blackmore_Springhaven_1887")) {
        for (workerCount in listOf(1, 2, 4, 8)) {
            println(run(fileName, workerCount, model))
        }
    }
}

private suspend fun run(
    fileName: String,
    workerCount: Int,
    model: TokenNameFinderModel
): Long {
    val outerJobScope = CoroutineScope(Dispatchers.Default)
    outerJobScope.launch {
        producer(this, fileName)
    }
    return measureTimeMillis {
        outerJobScope.launch {
            val jobScope = CoroutineScope(Dispatchers.Default)
            jobScope.launch {
                for (i in 0..workerCount) {
                    worker(this, model, "Worker-$i")
                }
            }
            jobScope.launch {
                aggregator(this)
            }.join()
        }.join()
    }
}