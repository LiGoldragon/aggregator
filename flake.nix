{
  description = "aggregator — evidence collection runtime scaffold";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs = { self, nixpkgs, flake-utils, fenix, crane }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        toolchain = fenix.packages.${system}.complete.withComponents [
          "cargo"
          "rustc"
          "rustfmt"
          "clippy"
          "rust-analyzer"
          "rust-src"
        ];
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
        examplesFilter = path: _type: builtins.match ".*/examples(/.*)?$" path != null;
        schemaFilter = path: _type: builtins.match ".*/schema(/.*)?$" path != null;
        generatedFilter = path: _type: builtins.match ".*/generated(/.*)?$" path != null;
        sourceFilter = path: type:
          (craneLib.filterCargoSources path type) || (examplesFilter path type) || (schemaFilter path type) || (generatedFilter path type);
        src = pkgs.lib.cleanSourceWith { src = ./.; filter = sourceFilter; name = "source"; };
        commonArgs = { inherit src; strictDeps = true; };
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;
      in
      {
        packages.default = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          meta.mainProgram = "aggregator";
        });
        checks = {
          build = craneLib.cargoBuild (commonArgs // { inherit cargoArtifacts; });
          test = craneLib.cargoTest (commonArgs // { inherit cargoArtifacts; });
          test-boundary = craneLib.cargoTest (commonArgs // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test boundary";
          });
          fmt = craneLib.cargoFmt { inherit src; };
          clippy = craneLib.cargoClippy (commonArgs // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- -D warnings";
          });
        };
        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/aggregator";
          meta.description = "Run the aggregator evidence collection CLI";
        };
        apps.daemon = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/aggregator-daemon";
          meta.description = "Run the aggregator daemon";
        };
        apps.meta = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/meta-aggregator";
          meta.description = "Run the aggregator meta-configuration CLI";
        };
        devShells.default = pkgs.mkShell {
          name = "aggregator";
          packages = [ pkgs.jujutsu toolchain ];
        };
      });
}
