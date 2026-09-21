"""ASCII-path entry point for the Windows batch launcher."""

from __future__ import annotations

from importlib import import_module


def main() -> None:
    module = import_module("\u767b\u5f55\u6001\u6355\u83b7\u52a9\u624b")
    module.main()


if __name__ == "__main__":
    main()
