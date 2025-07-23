{
  inputs,
  cell,
}: let
  inherit (inputs) std self cells nixpkgs;
  inherit (nixpkgs) dockerTools;
  crane = (inputs.crane.mkLib nixpkgs).overrideToolchain cells.core.rust.toolchain;

  version = self.dirtyRev or self.rev;

  btagger = crane.buildPackage {
    inherit version;
    pname = "btagger";
    meta.mainProgram = "btagger";

    src = std.incl self [
      "${self}/Cargo.lock"
      "${self}/Cargo.toml"
      "${self}/src"
    ];

    strictDeps = true;
  };
in {
  inherit btagger;
  default = btagger;
}
