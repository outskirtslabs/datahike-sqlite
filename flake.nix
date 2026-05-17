{
  description = "dev env for datahike-sqlite";

  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1"; # tracks nixpkgs unstable branch
    devshell.url = "github:numtide/devshell";
    devshell.inputs.nixpkgs.follows = "nixpkgs";
    devenv.url = "https://flakehub.com/f/ramblurr/nix-devenv/*";
    devenv.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    inputs@{
      self,
      devenv,
      devshell,
      ...
    }:
    devenv.lib.mkFlake ./. {
      inherit inputs;
      withOverlays = [
        devshell.overlays.default
        devenv.overlays.default
      ];
      packages = {
        default =
          pkgs:
          let
            jdk = "jdk25";
            jdkPackage = pkgs.${jdk};
            clojure = pkgs.clojure.override { jdk = jdkPackage; };
            root = toString ./.;
            gitRev =
              if self ? rev then
                self.rev
              else if self ? dirtyRev then
                self.dirtyRev
              else
                "dirty";
            projectSrc = pkgs.lib.cleanSourceWith {
              src = ./.;
              filter =
                path: _type:
                let
                  rel = pkgs.lib.removePrefix (root + "/") (toString path);
                  base = builtins.baseNameOf path;
                in
                !(
                  base == ".git"
                  || rel == "result"
                  || rel == ".tmuxb_session"
                  || rel == ".nrepl-port"
                  || pkgs.lib.hasPrefix ".cpcache/" rel
                  || pkgs.lib.hasPrefix ".clj-kondo/.cache/" rel
                  || pkgs.lib.hasPrefix ".clj-kondo/imports/" rel
                  || pkgs.lib.hasPrefix ".clj-kondo/inline-configs/" rel
                  || pkgs.lib.hasPrefix ".direnv/" rel
                  || pkgs.lib.hasPrefix ".lsp/" rel
                  || pkgs.lib.hasPrefix "bench/results/" rel
                  || pkgs.lib.hasPrefix "bench/tmp/" rel
                  || pkgs.lib.hasPrefix "extra/" rel
                  || pkgs.lib.hasPrefix "target/" rel
                  || pkgs.lib.hasPrefix "test-data/" rel
                );
            };
            clojureLocker = devenv.clojure.mkLockfile {
              inherit pkgs;
              jdk = jdkPackage;
              src = ./.;
              lockfile = "./deps-lock.json";
              extraPrepInputs = [ pkgs.git ];
            };
          in
          pkgs.stdenv.mkDerivation {
            pname = "datahike-sqlite";
            version = "0.0.1";
            src = projectSrc;
            nativeBuildInputs = [
              clojure
              pkgs.clj-kondo
              pkgs.cljfmt
              pkgs.coreutils
              pkgs.findutils
              pkgs.git
              jdkPackage
            ];
            GIT_REV = gitRev;
            JAVA_HOME = jdkPackage.home;
            buildPhase = ''
              runHook preBuild

              source ${clojureLocker.shellEnv}
              export GITLIBS="${clojureLocker.homeDirectory}/.gitlibs"
              export JAVA_HOME="${jdkPackage.home}"
              export JAVA_CMD="${jdkPackage}/bin/java"
              export JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS -Djava.io.tmpdir=$TMPDIR -Djna.tmpdir=$TMPDIR"

              clojure -Srepro -Xdeps prep :aliases '[:dev :test]'
              clj-kondo --fail-level error --lint src test bench
              clojure -Srepro -M:dev:test
              cljfmt -v check src test bench
              clojure -Srepro -T:build jar

              runHook postBuild
            '';
            installPhase = ''
              runHook preInstall

              mkdir -p "$out"
              cp "$(find target -type f -name '*.jar' -print | head -n 1)" "$out/"

              runHook postInstall
            '';
          };

        locker =
          pkgs:
          let
            jdk = "jdk25";
            jdkPackage = pkgs.${jdk};
            clojure = pkgs.clojure.override { jdk = jdkPackage; };
            clojureLocker = devenv.clojure.mkLockfile {
              inherit pkgs;
              jdk = jdkPackage;
              src = ./.;
              lockfile = "./deps-lock.json";
              extraPrepInputs = [ pkgs.git ];
            };
          in
          clojureLocker.commandLocker ''
            export HOME="$tmp/home"
            export GITLIBS="$tmp/home/.gitlibs"
            unset CLJ_CACHE CLJ_CONFIG XDG_CACHE_HOME XDG_CONFIG_HOME XDG_DATA_HOME

            ${clojure}/bin/clojure -Srepro -Xdeps prep :aliases '[:dev :test]'
            ${clojure}/bin/clojure -Srepro -P -M:dev:test
            ${clojure}/bin/clojure -Srepro -P -M:dev:bench
            ${clojure}/bin/clojure -Srepro -P -T:build jar
          '';
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
