import pandas as pd

import matplotlib.pyplot as plt

df = pd.read_csv('results.csv')

# Plotting
plt.figure(figsize=(8, 6))
plt.plot(df['workers'], df['sample'], label='sample', marker='o')
plt.plot(df['workers'], df['Lee_Albany_1884'], label='Lee_Albany_1884', marker='o')
plt.plot(df['workers'], df['Woolf_Years_1937'], label='Woolf_Years_1937', marker='o')
plt.plot(df['workers'], df['Blackmore_Springhaven_1887'], label='Blackmore_Springhaven_1887', marker='o')
plt.xlabel('workers')
plt.ylabel('time,ms')
plt.title('Experiments')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig('results.png')
