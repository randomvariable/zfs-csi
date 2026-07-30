{
  description = "zfs-csi dev + lint environment with libzfs (cgo) headers";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        # cgo toolchain + pkg-config so `#cgo pkg-config: libzfs libzfs_core`
        # resolves, and the userland libzfs/libspl headers + libs.
        nativeBuildInputs = [
          pkgs.go
          pkgs.gcc
          pkgs.pkg-config
        ];
        buildInputs = [
          pkgs.zfs
        ];

        CGO_ENABLED = "1";

        shellHook = ''
          echo "libzfs: $(pkg-config --modversion libzfs 2>/dev/null || echo MISSING)"
          echo "libzfs_core: $(pkg-config --modversion libzfs_core 2>/dev/null || echo MISSING)"
        '';
      };
    };
}
