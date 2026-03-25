import shutil
import subprocess
import sys
from pathlib import Path

from backend.core.app_info import APP_EXECUTABLE

ROOT = Path(__file__).resolve().parent
ENTRY_SCRIPT = ROOT / "main.py"
ASSETS_DIR = ROOT / "assets"
ICONS_DIR = ASSETS_DIR / "icons"


def _resolve_icon_path() -> Path:
    """Resolve the Windows/Linux icon file path."""
    icon_path = ICONS_DIR / "logo.ico"
    if not icon_path.exists():
        raise FileNotFoundError(f"未找到图标文件: {icon_path}")
    return icon_path


def _resolve_macos_icon() -> Path:
    """Resolve the macOS icon file path."""
    icns_path = ICONS_DIR / "logo.icns"
    if not icns_path.exists():
        raise FileNotFoundError(
            f"未找到 macOS 图标文件: {icns_path}\n"
            f"请确保已在 assets/icons/ 中放置 logo.icns 文件"
        )
    return icns_path


ICON_PATH = _resolve_icon_path()


def run_command(cmd):
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, cwd=str(ROOT), check=True)


def clean_dir(path: Path):
    if path.exists():
        shutil.rmtree(path)


def build_windows_or_macos():
    cmd = [
        sys.executable,
        "-m",
        "nuitka",
        "--standalone",
        "--assume-yes-for-downloads",
        "--enable-plugin=pyside6",
        f"--include-data-files={ICON_PATH}=logo.ico",
        f"--output-dir={ROOT / 'dist'}",
        f"--output-filename={APP_EXECUTABLE}",
    ]

    if sys.platform.startswith("win"):
        cmd.extend(
            [
                "--windows-console-mode=disable",
                f"--windows-icon-from-ico={ICON_PATH}",
            ]
        )
    elif sys.platform == "darwin":
        cmd.extend(
            [
                "--macos-create-app-bundle",
                f"--macos-app-name={APP_EXECUTABLE}",
                f"--macos-app-icon={_resolve_macos_icon()}",
            ]
        )

    cmd.append(str(ENTRY_SCRIPT))
    run_command(cmd)


def build_linux():
    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        f"--name={APP_EXECUTABLE}",
        "--windowed",
        "--collect-all",
        "qfluentwidgets",
        "--icon",
        str(ICON_PATH),
        "--add-data",
        f"{ICON_PATH}{';' if sys.platform.startswith('win') else ':'}.",
        str(ENTRY_SCRIPT),
    ]
    run_command(cmd)


def rename_macos_app():
    """Rename main.app to SilkLoom.app if needed (Nuitka quirk on macOS)"""
    if sys.platform == "darwin":
        main_app = ROOT / "dist" / "main.app"
        target_app = ROOT / "dist" / f"{APP_EXECUTABLE}.app"
        
        if main_app.exists() and not target_app.exists():
            print(f"Renaming {main_app.name} to {target_app.name}")
            main_app.rename(target_app)
        
        # Clean up build artifacts
        build_dir = ROOT / "dist" / "main.build"
        if build_dir.exists():
            print(f"Cleaning up {build_dir.name}")
            clean_dir(build_dir)


def main():
    clean_dir(ROOT / "build")

    if sys.platform.startswith("linux"):
        build_linux()
    elif sys.platform.startswith("win") or sys.platform == "darwin":
        build_windows_or_macos()
        rename_macos_app()
    else:
        raise RuntimeError(f"Unsupported platform: {sys.platform}")


if __name__ == "__main__":
    main()
