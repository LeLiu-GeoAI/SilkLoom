import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QIcon
from qfluentwidgets import Theme, setTheme

from backend.core.app_info import APP_NAME

# Ensure project root is importable when running this file directly.
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from main_window import MainWindow


def _resolve_logo_icon() -> Path | None:
    base_candidates = [
        ROOT,
        Path(sys.executable).resolve().parent,
    ]

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        base_candidates.append(Path(meipass))

    for base in base_candidates:
        candidate = base / "logo.ico"
        if candidate.exists():
            return candidate

    return None


def main():
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)

    icon_path = _resolve_logo_icon()
    if icon_path is not None:
        app.setWindowIcon(QIcon(str(icon_path)))

    setTheme(Theme.AUTO)

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
