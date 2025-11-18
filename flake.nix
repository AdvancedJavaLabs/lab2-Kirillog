{
  description = "Rabbit MQ flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem ( system:
    let
      pkgs = import nixpkgs { inherit system; };
    in {
      devShell = pkgs.mkShell {
        name = "rabbitmq";
        packages = with pkgs; [
			rabbitmq-server
			(pkgs.python3.withPackages (ps: with ps; [ matplotlib pandas ]))
        ];
      };
    });
}

