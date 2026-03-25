from PySide6.QtCore import QThread, Signal


class RuntimeWorker(QThread):
    """Run RuntimeService.start_processing in a Qt thread."""

    # 关键优化: 快速启动状态反馈
    initializing = Signal()  # 真正启动时触发（早于任何处理）
    event_emitted = Signal(dict)
    failed = Signal(str)

    def __init__(self, runtime_service, yaml_content: str, file_path: str, parent=None):
        super().__init__(parent)
        self._runtime_service = runtime_service
        self._yaml_content = yaml_content
        self._file_path = file_path

    def run(self):
        try:
            # 立即发送初始化信号，提供快速视觉反馈
            self.initializing.emit()
            
            for event in self._runtime_service.start_processing(self._yaml_content, self._file_path):
                self.event_emitted.emit(event)
        except Exception as e:  # pragma: no cover
            self.failed.emit(str(e))
