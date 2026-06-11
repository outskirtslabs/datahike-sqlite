{
  description = "dev env for datahike-sqlite";

  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1"; # tracks nixpkgs unstable branch
    devshell.url = "github:numtide/devshell";
    devshell.inputs.nixpkgs.follows = "nixpkgs";
    devenv.url = "github:ramblurr/nix-devenv";
    devenv.inputs.nixpkgs.follows = "nixpkgs";
    clj-helpers.url = "github:outskirtslabs/clojure-nix-locker-helpers";
    clj-helpers.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    inputs@{
      self,
      devenv,
      devshell,
      clj-helpers,
      ...
    }:
    let
      package =
        pkgs:
        clj-helpers.lib.mkCljLib {
          inherit pkgs;
          name = "datahike-sqlite";
          version = "0.0.1";
          src = ./.;
          extraSrcExcludes = [
            "bench/results"
            "bench/tmp"
            "test-data"
          ];
          prepAliases = [
            "dev"
            "test"
          ];
          prefetchAliases = [
            "dev:test"
            "dev:bench"
          ];
          checkCommand = ''
            export JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS -Djna.tmpdir=$TMPDIR"
            clj-kondo --fail-level error --lint src test bench
            clojure -Srepro -M:dev:test
            cljfmt -v check src test bench
          '';
          gitRev = clj-helpers.lib.gitRev self;
          nativeBuildInputs = [
            pkgs.clj-kondo
            pkgs.cljfmt
          ];
        };
    in
    devenv.lib.mkFlake ./. {
      inherit inputs;
      withOverlays = [
        devshell.overlays.default
        devenv.overlays.default
      ];
      packages = {
        default = package;
        # regenerates ./deps-lock.json: `nix run .#locker`
        locker = pkgs: (package pkgs).locker;
      };
      devShell =
        pkgs:
        pkgs.devshell.mkShell {
          imports = [
            devenv.capsules.base
            devenv.capsules.clojure
          ];
          env = [
            {
              name = "JAVA_HOME";
              value = pkgs.jdk25.home;
            }
          ];
          packages = [
            pkgs.clj-kondo
            pkgs.cljfmt
            pkgs.clojure-lsp
            pkgs.git
            pkgs.glib
            pkgs.jdk25
            pkgs.vips
            self.packages.${pkgs.system}.locker
          ];
        };
    };
}
