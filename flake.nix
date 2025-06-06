{
  description = "hsfive";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-25.05";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      top-level-pkgs = import nixpkgs {
        inherit system;
        overlays = [ ];
      };

      haskellPackages = top-level-pkgs.haskellPackages.override
        {
          # This is a hack. We don't need hdf5 from hackage at all, but "pkgconfig-depends" in the cabal file
          # forces the usage of hdf5 somehow. So we map it to an existing, used package and everything works.
          overrides = self: super: { };
        };

      packageName = "hsfive";
    in
    {
      packages.${system}.default =
        haskellPackages.callCabal2nix packageName self
          { };

      devShells.${system}.default =
        top-level-pkgs.mkShell {
          buildInputs = with top-level-pkgs;
            [
              haskellPackages.haskell-language-server # you must build it with your ghc to work
              cabal-install
              haskellPackages.cabal-plan
              haskellPackages.hlint
              haskellPackages.apply-refact
            ];
          inputsFrom = [ self.packages.${system}.default.env ];
        };
    };

}
