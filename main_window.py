import os
import json
import sys
from pathlib import Path
import yaml

from PySide6.QtCore import Qt, QTimer, QRegularExpression, QUrl
from PySide6.QtGui import QColor, QFont, QTextCharFormat, QSyntaxHighlighter, QPixmap, QDesktopServices, QGuiApplication
from PySide6.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from qfluentwidgets import (
    ComboBox,
    CardWidget,
    SpinBox,
    DoubleSpinBox,
    FluentIcon as FIF,
    FluentWindow,
    InfoBar,
    InfoBarPosition,
    LineEdit,
    NavigationItemPosition,
    PlainTextEdit,
    PrimaryPushButton,
    ProgressBar,
    PushButton,
    StrongBodyLabel,
    TableWidget,
)
from PySide6.QtWidgets import QCheckBox

from backend.config_io import load_default_yaml, save_yaml
from backend.core.extractor import export_results_to_file
from backend.core.model_repository import (
    delete_model,
    get_default_model,
    get_model_by_name,
    init_model_db,
    list_models,
    upsert_model,
)
from backend.core.task_repository import reconcile_stale_task_statuses
from backend.core.template_tools import analyze_template_requirements
from backend.services.analysis_service import FileAnalysisService
from backend.services.runtime_service import RuntimeService
from backend.services.task_service import TaskService
from backend.core.performance_tools import Debouncer, AsyncTaskRunner, TableOptimizer
from backend.core.app_info import (
    APP_NAME,
    APP_VERSION,
    APP_DESCRIPTION,
    APP_LICENSE,
    APP_COPYRIGHT,
    APP_SUPPORT_EMAIL,
    APP_REPO_URL,
)
from jinja2 import Template

from worker import RuntimeWorker
from schema_editor_dialog import SchemaEditorDialog


def _represent_multiline_str(dumper, data):
    """Custom representer for multiline strings in YAML"""
    if '\n' in data:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


class CustomYamlDumper(yaml.SafeDumper):
    """Custom YAML dumper for better formatting of multiline strings"""
    pass


CustomYamlDumper.add_representer(str, _represent_multiline_str)


# Register the custom representer for multiline strings
yaml.add_representer(str, _represent_multiline_str)


class Jinja2SyntaxHighlighter(QSyntaxHighlighter):
    """Jinja2 templates with enhanced syntax highlighting"""
    def __init__(self, parent):
        super().__init__(parent)
        self._rules = []

        # Jinja2 标签: {% ... %}
        jinja_tag_format = QTextCharFormat()
        jinja_tag_format.setForeground(QColor("#7C3AED"))  # 紫色：标签背景
        jinja_tag_format.setFontWeight(QFont.Weight.Medium)
        self._rules.append((QRegularExpression(r"\{%.*?%\}"), jinja_tag_format))

        # Jinja2 变量: {{ ... }}
        jinja_var_format = QTextCharFormat()
        jinja_var_format.setForeground(QColor("#0D9488"))  # 青色：变量
        jinja_var_format.setFontWeight(QFont.Weight.Medium)
        self._rules.append((QRegularExpression(r"\{\{.*?\}\}"), jinja_var_format))

        # Jinja2 过滤器: | 之后的部分
        jinja_filter_format = QTextCharFormat()
        jinja_filter_format.setForeground(QColor("#EA580C"))  # 橙色：过滤器
        jinja_filter_format.setFontItalic(True)
        self._rules.append((QRegularExpression(r"\|\s*\w+"), jinja_filter_format))

        # 字符串 (引号内)
        string_format = QTextCharFormat()
        string_format.setForeground(QColor("#B91C1C"))  # 深红色：字符串
        self._rules.append((QRegularExpression(r'["\'](?:\\.|[^"\'\\])*["\']'), string_format))

        # 注释
        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor("#6B7280"))  # 灰色：注释
        comment_format.setFontItalic(True)
        self._rules.append((QRegularExpression(r"#.*$"), comment_format))

    def highlightBlock(self, text):
        for regex, text_format in self._rules:
            it = regex.globalMatch(text)
            while it.hasNext():
                match = it.next()
                self.setFormat(match.capturedStart(), match.capturedLength(), text_format)


class MainWindow(FluentWindow):
    def __init__(self):
        super().__init__()

        reconcile_stale_task_statuses()
        init_model_db()

        self.analysis_service = FileAnalysisService()
        self.runtime_service = RuntimeService()
        self.task_service = TaskService()
        self.worker = None
        
        # 性能优化：防抖分析和异步任务
        self.analyze_debouncer = Debouncer(wait_ms=500)
        self.async_runner = AsyncTaskRunner(max_workers=2)
        self.async_runner.task_finished.connect(self._on_analysis_finished)
        self.async_runner.task_failed.connect(self._on_analysis_failed)
        self.config_save_timer = QTimer(self)
        self.config_save_timer.setSingleShot(True)
        self.config_save_timer.setInterval(700)
        self.config_save_timer.timeout.connect(self._persist_yaml_config)
        
        # 性能优化：缓存上一次轮询状态，避免不必要的UI更新
        self._last_poll_state = {
            'percent': -1,
            'log': None,
            'status': None,
            'running': False
        }
        self._base_runtime_config = {}
        self._task_summary_last = ""
        self._selected_model_row_id = ""
        self._model_records = []
        self._task_selectors = []
        self._available_template_vars = []
        self._task_extra_config = {}
        
        # 新增：GUI配置状态变量
        self._current_schema = {}  # 目标列名字典
        self._current_prompt_template = ""  # 提示词模板文本

        self.setWindowTitle("SilkLoom（蚕小织）- 批量文本提取、分类与总结")
        self._resize_for_screen(1220, 620)
        self.setMinimumSize(900, 460)

        self._build_pages()
        self._apply_workspace_styles()
        self._bind_events()
        self._bootstrap_data()

        # 备用轮询机制：10秒一次（优化：从 5 秒改为 10 秒，减少 CPU 占用）
        # 主要依靠事件驱动，轮询作为容错机制
        self.poll_timer = QTimer(self)
        self.poll_timer.setInterval(10000)  # 10 秒，而不是 5 秒
        self.poll_timer.timeout.connect(self.poll_runtime)
        self.poll_timer.start()

    def _resize_for_screen(self, preferred_width: int, preferred_height: int):
        """根据当前屏幕可用区域限制窗口尺寸，避免小屏幕显示不全。"""
        screen = self.screen() or QGuiApplication.primaryScreen()
        if screen is None:
            self.resize(preferred_width, preferred_height)
            return

        available = screen.availableGeometry()
        max_width = max(900, int(available.width() * 0.92))
        max_height = max(440, int(available.height() * 0.86))
        self.resize(min(preferred_width, max_width), min(preferred_height, max_height))

    def _build_pages(self):
        self.workspace_page = QWidget()
        self.workspace_page.setObjectName("workspacePage")
        self.task_page = QWidget()
        self.task_page.setObjectName("taskPage")
        self.model_page = QWidget()
        self.model_page.setObjectName("modelPage")
        self.about_page = QWidget()
        self.about_page.setObjectName("aboutPage")

        self._build_workspace_page()
        self._build_task_page()
        self._build_model_page()
        self._build_about_page()

        self.addSubInterface(self.workspace_page, FIF.HOME, "工作台")
        self.addSubInterface(self.task_page, FIF.LIBRARY, "任务管理", NavigationItemPosition.TOP)
        self.addSubInterface(self.model_page, FIF.ROBOT, "模型管理", NavigationItemPosition.TOP)
        self.addSubInterface(self.about_page, FIF.INFO, "关于", NavigationItemPosition.BOTTOM)

    def _resolve_runtime_asset(self, rel_path: str) -> Path | None:
        base_candidates = [
            Path(__file__).resolve().parent,
            Path(sys.executable).resolve().parent,
        ]

        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            base_candidates.append(Path(meipass))

        for base in base_candidates:
            candidate = base / rel_path
            if candidate.exists():
                return candidate
        return None

    def _build_about_page(self):
        root = QVBoxLayout(self.about_page)
        root.setContentsMargins(14, 10, 14, 10)
        root.setSpacing(8)

        about_card, about_layout = self._card("关于 SilkLoom", accent="#10B981")

        logo_label = QLabel()
        logo_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        logo_path = self._resolve_runtime_asset("logo.ico")
        if logo_path:
            pixmap = QPixmap(str(logo_path))
            if not pixmap.isNull():
                logo_label.setPixmap(
                    pixmap.scaled(
                        96,
                        96,
                        Qt.AspectRatioMode.KeepAspectRatio,
                        Qt.TransformationMode.SmoothTransformation,
                    )
                )
            else:
                logo_label.setText(APP_NAME)
        else:
            logo_label.setText(APP_NAME)
        about_layout.addWidget(logo_label)

        title = QLabel(f"{APP_NAME} v{APP_VERSION}")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title.setObjectName("aboutTitleLabel")
        about_layout.addWidget(title)

        desc = QLabel(APP_DESCRIPTION)
        desc.setAlignment(Qt.AlignmentFlag.AlignCenter)
        desc.setWordWrap(True)
        desc.setObjectName("aboutDescriptionLabel")
        about_layout.addWidget(desc)

        info_text = QLabel(
            "\n".join(
                [
                    f"许可证: {APP_LICENSE}",
                    APP_COPYRIGHT,
                    f"支持邮箱: {APP_SUPPORT_EMAIL}",
                ]
            )
        )
        info_text.setAlignment(Qt.AlignmentFlag.AlignCenter)
        info_text.setWordWrap(True)
        info_text.setStyleSheet(
            "color: #475569; background: rgba(15, 23, 42, 0.03);"
            "border: 1px solid rgba(15, 23, 42, 0.08); border-radius: 8px; padding: 10px;"
        )
        about_layout.addWidget(info_text)

        mail_link = QLabel(
            f'<a href="mailto:{APP_SUPPORT_EMAIL}">联系支持</a> | '
            f'<a href="{APP_REPO_URL}">GitHub</a>'
        )
        mail_link.setAlignment(Qt.AlignmentFlag.AlignCenter)
        mail_link.setOpenExternalLinks(False)
        mail_link.linkActivated.connect(lambda url: QDesktopServices.openUrl(QUrl(url)))
        about_layout.addWidget(mail_link)

        about_layout.addStretch(1)
        root.addWidget(about_card, 1)

    def _build_workspace_page(self):
        root = QVBoxLayout(self.workspace_page)
        root.setContentsMargins(14, 10, 14, 10)
        root.setSpacing(8)

        split_row = QHBoxLayout()
        split_row.setSpacing(8)

        left_wrap = QWidget()
        left_column = QVBoxLayout(left_wrap)
        left_column.setSpacing(6)
        left_column.setContentsMargins(0, 0, 0, 0)

        right_wrap = QWidget()
        right_column = QVBoxLayout(right_wrap)
        right_column.setSpacing(6)
        right_column.setContentsMargins(0, 0, 0, 0)

        settings_card, settings_layout = self._card("任务配置", accent="#3B82F6")
        settings_row = QHBoxLayout()
        settings_row.setSpacing(10)

        self.model_selector = ComboBox()
        self.worker_spin = SpinBox()
        self.temperature_spin = DoubleSpinBox()
        self.timeout_spin = SpinBox()
        self.max_tokens_spin = SpinBox()
        self.task_name_edit = LineEdit()
        self.enable_think_checkbox = QCheckBox("Think 模式")

        self.model_selector.setMinimumWidth(120)
        self.worker_spin.setMinimumWidth(80)
        self.worker_spin.setRange(1, 32)
        self.worker_spin.setValue(10)
        self.temperature_spin.setMinimumWidth(100)
        self.temperature_spin.setRange(0.0, 2.0)
        self.temperature_spin.setValue(0.0)
        self.temperature_spin.setSingleStep(0.1)
        self.temperature_spin.setDecimals(1)
        self.timeout_spin.setMinimumWidth(80)
        self.timeout_spin.setRange(1, 600)
        self.timeout_spin.setValue(60)
        self.max_tokens_spin.setMinimumWidth(120)
        self.max_tokens_spin.setRange(128, 32768)
        self.max_tokens_spin.setSingleStep(128)
        self.task_name_edit.setMinimumWidth(260)
        self.task_name_edit.setPlaceholderText("可留空，自动命名")

        settings_row.addWidget(QLabel("模型"))
        settings_row.addWidget(self.model_selector)
        settings_row.addWidget(QLabel("温度"))
        settings_row.addWidget(self.temperature_spin)
        settings_row.addWidget(QLabel("Max Tokens"))
        settings_row.addWidget(self.max_tokens_spin)
        settings_row.addWidget(self.enable_think_checkbox)
        settings_row.addStretch(1)
        settings_layout.addLayout(settings_row)

        task_name_row = QHBoxLayout()
        task_name_row.setSpacing(10)
        task_name_row.addWidget(QLabel("任务名"))
        task_name_row.addWidget(self.task_name_edit, 1)
        task_name_row.addWidget(QLabel("并发"))
        task_name_row.addWidget(self.worker_spin)
        task_name_row.addWidget(QLabel("超时(s)"))
        task_name_row.addWidget(self.timeout_spin)
        task_name_row.addStretch(1)
        settings_layout.addLayout(task_name_row)
        left_column.addWidget(settings_card)

        config_card, config_layout = self._card("提示词配置", accent="#0EA5E9")
        self.task_summary_label = QLabel("等待配置")
        self.task_summary_label.setObjectName("taskSummaryLabel")
        self.task_summary_label.setWordWrap(True)
        config_layout.addWidget(self.task_summary_label)

        # 输出列名说明配置区域
        schema_section = QHBoxLayout()
        schema_label = QLabel("输出列名:")
        self.schema_display_label = QLabel("未配置")
        self.schema_display_label.setStyleSheet("color: #666; font-size: 11px;")
        self.schema_config_button = PrimaryPushButton("配置输出列名")
        self.schema_config_button.clicked.connect(self._open_schema_editor)
        schema_section.addWidget(schema_label)
        schema_section.addWidget(self.schema_display_label, 1)
        schema_section.addWidget(self.schema_config_button)
        config_layout.addLayout(schema_section)

        # 提示词模板编辑区域
        template_label = QLabel("提示词模板:")
        config_layout.addWidget(template_label)

        self.prompt_edit = PlainTextEdit()
        self.prompt_highlighter = Jinja2SyntaxHighlighter(self.prompt_edit.document())
        self.prompt_edit.setMinimumHeight(180)
        config_layout.addWidget(self.prompt_edit, 1)

        # 模板变量GUI插入器
        var_row = QHBoxLayout()
        var_row.setSpacing(8)
        self.template_var_selector = ComboBox()
        self.template_var_selector.setMinimumWidth(280)
        self.insert_var_button = PushButton("插入列名变量")
        self.insert_schema_button = PushButton("插入输出列名")
        self.insert_if_block_button = PushButton("插入条件块")
        var_row.addWidget(QLabel("列名变量"))
        var_row.addWidget(self.template_var_selector, 1)
        var_row.addWidget(self.insert_var_button)
        var_row.addWidget(self.insert_schema_button)
        var_row.addWidget(self.insert_if_block_button)
        config_layout.addLayout(var_row)

        action_row = QHBoxLayout()
        action_row.setSpacing(8)
        self.template_validation_label = QLabel("待检查")
        self.template_validation_label.setWordWrap(True)
        self.template_validation_label.setStyleSheet("color: #64748B; font-size: 11px;")
        self.preview_prompt_button = PrimaryPushButton("预览提示词")
        self.preview_prompt_button.setToolTip("用示例数据预览")
        self.import_config_button = PushButton("导入配置")
        self.export_config_button = PushButton("导出配置")
        action_row.addWidget(self.template_validation_label, 1)
        action_row.addStretch(1)
        action_row.addWidget(self.preview_prompt_button)
        action_row.addWidget(self.import_config_button)
        action_row.addWidget(self.export_config_button)
        config_layout.addLayout(action_row)
        
        left_column.addWidget(config_card, 1)

        source_card, source_layout = self._card("数据源", accent="#14B8A6")
        source_row = QHBoxLayout()
        self.file_path_edit = LineEdit()
        self.file_path_edit.setPlaceholderText("例如: D:/data/papers.xlsx")
        self.browse_button = PushButton("浏览...")
        source_row.addWidget(self.file_path_edit)
        source_row.addWidget(self.browse_button)
        source_layout.addLayout(source_row)

        self.stats_label = QLabel("请选择文件")
        self.stats_label.setWordWrap(False)
        source_layout.addWidget(self.stats_label)

        self.preview_table = TableWidget()
        self.preview_table.setColumnCount(5)
        self.preview_table.setHorizontalHeaderLabels(["列名 (列名)", "有效值", "缺失值", "类型", "示例值"])
        self.preview_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        preview_header = self.preview_table.horizontalHeader()
        if preview_header is not None:
            preview_header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.preview_table.setMinimumHeight(100)
        source_layout.addWidget(self.preview_table)
        right_column.addWidget(source_card, 4)

        run_card, run_layout = self._card("运行监控", accent="#F59E0B")
        run_row = QHBoxLayout()
        self.run_button = PrimaryPushButton("开始运行")
        self.stop_button = PushButton("停止运行")
        self.run_button.setEnabled(False)
        self.stop_button.setEnabled(False)
        run_row.addWidget(self.run_button)
        run_row.addWidget(self.stop_button)
        run_layout.addLayout(run_row)

        self.status_label = QLabel("等待运行")
        self.status_label.setWordWrap(True)
        self.progress_bar = ProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)

        self.log_text = PlainTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMinimumHeight(64)

        run_layout.addWidget(self.status_label)
        run_layout.addWidget(self.progress_bar)
        run_layout.addWidget(self.log_text, 1)
        right_column.addWidget(run_card, 6)

        export_card, export_layout = self._card("结果导出", accent="#6366F1")
        export_row = QHBoxLayout()
        self.export_format = ComboBox()
        self.export_format.addItems(["CSV", "Excel", "JSONL"])
        self.generate_export_button = PushButton("生成导出文件")
        self.open_export_button = PushButton("打开导出文件")
        self.open_export_button.setEnabled(False)
        export_row.addWidget(self.export_format)
        export_row.addWidget(self.generate_export_button)
        export_row.addWidget(self.open_export_button)
        export_layout.addLayout(export_row)

        self.export_path_label = QLabel("尚未生成导出文件")
        self.export_path_label.setWordWrap(True)
        export_layout.addWidget(self.export_path_label)
        right_column.addWidget(export_card, 2)

        split_row.addWidget(left_wrap, 6)
        split_row.addWidget(right_wrap, 5)
        root.addLayout(split_row, 1)

    def _build_task_page(self):
        root = QVBoxLayout(self.task_page)
        root.setContentsMargins(14, 10, 14, 10)
        root.setSpacing(8)

        top_row = QHBoxLayout()
        self.refresh_tasks_button = PushButton("刷新")
        self.load_task_button = PrimaryPushButton("加载")
        self.delete_task_button = PushButton("删除所选任务")
        self.clear_task_button = PushButton("清空所选结果")

        tip = QLabel("请在下方表格中选择任务")
        tip.setStyleSheet("color: #475569;")
        top_row.addWidget(tip, 3)
        top_row.addWidget(self.refresh_tasks_button)
        top_row.addWidget(self.load_task_button)
        top_row.addWidget(self.delete_task_button)
        top_row.addWidget(self.clear_task_button)
        root.addLayout(top_row)

        self.task_table = TableWidget()
        self.task_table.setColumnCount(7)
        self.task_table.setHorizontalHeaderLabels(["任务名", "状态", "数据文件", "总行数", "成功", "失败", "更新时间"])
        self.task_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.task_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.task_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        task_header = self.task_table.horizontalHeader()
        if task_header is not None:
            task_header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.task_table.setMinimumHeight(150)
        root.addWidget(self.task_table)

        self.task_action_label = QLabel("")
        self.task_action_label.setWordWrap(True)
        root.addWidget(self.task_action_label)

    def _build_model_page(self):
        root = QVBoxLayout(self.model_page)
        root.setContentsMargins(14, 10, 14, 10)
        root.setSpacing(8)

        table_card, table_layout = self._card("模型列表", accent="#0EA5E9")
        self.model_table = TableWidget()
        self.model_table.setColumnCount(6)
        self.model_table.setHorizontalHeaderLabels(["展示名", "模型", "URL", "代理", "Key", "默认"])
        self.model_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        model_header = self.model_table.horizontalHeader()
        if model_header is not None:
            model_header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.model_table.setMinimumHeight(150)
        table_layout.addWidget(self.model_table)
        root.addWidget(table_card)

        edit_card, edit_layout = self._card("编辑模型", accent="#64748B")
        form_col = QVBoxLayout()
        form_col.setSpacing(8)

        row_1 = QHBoxLayout()
        self.model_name_edit = LineEdit()
        self.model_name_edit.setPlaceholderText("展示名，例如：研究模型-A")
        self.model_name_raw_edit = LineEdit()
        self.model_name_raw_edit.setPlaceholderText("模型名，例如：glm-4-flash")
        row_1.addWidget(QLabel("展示名"))
        row_1.addWidget(self.model_name_edit, 1)
        row_1.addWidget(QLabel("模型"))
        row_1.addWidget(self.model_name_raw_edit, 1)

        row_2 = QHBoxLayout()
        self.model_url_edit = LineEdit()
        self.model_url_edit.setPlaceholderText("Base URL")
        row_2.addWidget(QLabel("URL"))
        row_2.addWidget(self.model_url_edit, 1)

        row_3 = QHBoxLayout()
        self.model_key_edit = LineEdit()
        self.model_key_edit.setPlaceholderText("API Key")
        row_3.addWidget(QLabel("Key"))
        row_3.addWidget(self.model_key_edit, 1)

        row_4 = QHBoxLayout()
        self.model_proxy_edit = LineEdit()
        self.model_proxy_edit.setPlaceholderText("代理 URL（可选），如：http://127.0.0.1:7890")
        row_4.addWidget(QLabel("代理"))
        row_4.addWidget(self.model_proxy_edit, 1)

        action_row = QHBoxLayout()
        self.new_model_button = PushButton("新建")
        self.save_model_button = PrimaryPushButton("保存")
        self.set_default_model_button = PushButton("设为默认")
        self.delete_model_button = PushButton("删除")
        self.refresh_model_button = PushButton("刷新")
        action_row.addWidget(self.new_model_button)
        action_row.addWidget(self.save_model_button)
        action_row.addWidget(self.set_default_model_button)
        action_row.addWidget(self.delete_model_button)
        action_row.addWidget(self.refresh_model_button)
        action_row.addStretch(1)

        form_col.addLayout(row_1)
        form_col.addLayout(row_2)
        form_col.addLayout(row_3)
        form_col.addLayout(row_4)
        form_col.addLayout(action_row)
        edit_layout.addLayout(form_col)
        root.addWidget(edit_card)

    def _bind_events(self):
        self.browse_button.clicked.connect(self.pick_file)
        # 优化：为输入框添加防抖，避免频繁分析
        self.file_path_edit.textChanged.connect(
            lambda: self.analyze_debouncer.call()
        )
        self.prompt_edit.textChanged.connect(
            lambda: self.analyze_debouncer.call()
        )
        self.prompt_edit.textChanged.connect(self._update_task_summary)
        self.prompt_edit.textChanged.connect(self._schedule_config_save)
        self.prompt_edit.textChanged.connect(self._refresh_template_var_selector)
        self.prompt_edit.textChanged.connect(lambda: self._validate_prompt_template(show_notify=False))
        self.insert_var_button.clicked.connect(self._insert_selected_template_var)
        self.insert_schema_button.clicked.connect(self._insert_schema_placeholder)
        self.insert_if_block_button.clicked.connect(self._insert_if_block_template)
        self.preview_prompt_button.clicked.connect(self._preview_full_prompt)
        self.import_config_button.clicked.connect(self._import_config_file)
        self.export_config_button.clicked.connect(self._export_config_file)

        self.model_selector.currentTextChanged.connect(self._update_task_summary)
        self.model_selector.currentTextChanged.connect(lambda: self.analyze_debouncer.call())
        self.worker_spin.valueChanged.connect(self._update_task_summary)
        self.worker_spin.valueChanged.connect(lambda _: self.analyze_debouncer.call())
        self.temperature_spin.valueChanged.connect(lambda _: self.analyze_debouncer.call())
        self.timeout_spin.valueChanged.connect(lambda _: self.analyze_debouncer.call())
        self.max_tokens_spin.valueChanged.connect(lambda _: self.analyze_debouncer.call())
        self.enable_think_checkbox.stateChanged.connect(lambda _: self.analyze_debouncer.call())
        self.model_selector.currentTextChanged.connect(self._schedule_config_save)
        self.worker_spin.valueChanged.connect(self._schedule_config_save)
        self.temperature_spin.valueChanged.connect(lambda _: self._schedule_config_save())
        self.timeout_spin.valueChanged.connect(self._schedule_config_save)
        self.max_tokens_spin.valueChanged.connect(self._schedule_config_save)
        self.enable_think_checkbox.stateChanged.connect(lambda _: self._schedule_config_save())
        self.task_name_edit.textChanged.connect(self._update_task_summary)
        self.task_name_edit.textChanged.connect(self._schedule_config_save)

        self.analyze_debouncer.add_callback(self.analyze_current_input)

        self.run_button.clicked.connect(self.start_runtime)
        self.stop_button.clicked.connect(self.stop_runtime)

        self.generate_export_button.clicked.connect(self.generate_export)
        self.open_export_button.clicked.connect(self.open_export)

        self.refresh_tasks_button.clicked.connect(self.refresh_tasks)
        self.load_task_button.clicked.connect(self.load_selected_task)
        self.delete_task_button.clicked.connect(self.delete_selected_task)
        self.clear_task_button.clicked.connect(self.clear_selected_task)

        self.model_table.itemSelectionChanged.connect(self._on_model_row_selected)
        self.new_model_button.clicked.connect(self._new_model_profile)
        self.save_model_button.clicked.connect(self._save_model_profile)
        self.set_default_model_button.clicked.connect(lambda: self._save_model_profile(make_default=True))
        self.delete_model_button.clicked.connect(self._delete_model_profile)
        self.refresh_model_button.clicked.connect(self.refresh_model_profiles)

    def _bootstrap_data(self):
        self.refresh_tasks()
        self.refresh_model_profiles()
        cfg = self.runtime_service.get_runtime_task_bootstrap_data()
        if cfg:
            selector = f"{cfg['task_name']} | {cfg['hash'][:8]} | {cfg['status']}"
            self._set_task_selector(selector)
            self._apply_full_config_to_controls(cfg.get("yaml_config", ""))
            self.file_path_edit.setText(cfg.get("file_path", ""))
            self._notify("已恢复运行任务", f"{cfg['task_name']} ({cfg['hash'][:8]})", "success")
        else:
            self._apply_full_config_to_controls(load_default_yaml())
        self._update_task_summary()
        self._refresh_template_var_selector()
        self._validate_prompt_template(show_notify=False)
        self.analyze_current_input()
        self.poll_runtime()

    def _card(self, title: str, accent: str = "#0EA5E9") -> tuple[CardWidget, QVBoxLayout]:
        frame = CardWidget()
        frame.setObjectName("workspaceCard")

        outer = QVBoxLayout(frame)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        header = QFrame()
        header.setObjectName("workspaceCardHeader")
        header.setStyleSheet(
            f"""
            QFrame#workspaceCardHeader {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 {accent}2A, stop:1 rgba(255, 255, 255, 0.92));
                border-bottom: 1px solid rgba(15, 23, 42, 0.11);
                border-top-left-radius: 12px;
                border-top-right-radius: 12px;
            }}
            """
        )
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(10, 6, 10, 6)
        header_layout.setSpacing(6)

        label = StrongBodyLabel(title)
        label.setObjectName("workspaceCardPillTitle")
        label.setStyleSheet(
            f"""
            StrongBodyLabel#workspaceCardPillTitle {{
                color: #0f172a;
                background-color: #ffffff;
                border: 1px solid {accent}66;
                border-radius: 10px;
                padding: 2px 10px;
                font-size: 13px;
                font-weight: 700;
            }}
            """
        )
        header_layout.addWidget(label)
        header_layout.addStretch(1)

        content = QWidget()
        content.setObjectName("workspaceCardContent")

        layout = QVBoxLayout(content)
        layout.setContentsMargins(10, 7, 10, 8)
        layout.setSpacing(5)

        outer.addWidget(header)
        outer.addWidget(content)
        return frame, layout

    def _safe_load_yaml(self, yaml_text: str) -> dict:
        try:
            data = yaml.safe_load(yaml_text)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _task_yaml_to_text(self, task_dict: dict) -> str:
        return yaml.dump({"task": task_dict}, Dumper=CustomYamlDumper, allow_unicode=True, sort_keys=False)

    def _set_combo_value(self, combo: ComboBox, value: str):
        for i in range(combo.count()):
            if combo.itemText(i) == value:
                combo.setCurrentIndex(i)
                return
        combo.addItem(value)
        combo.setCurrentIndex(combo.count() - 1)

    def _apply_full_config_to_controls(self, full_yaml_text: str):
        parsed = self._safe_load_yaml(full_yaml_text)
        if not parsed:
            parsed = self._safe_load_yaml(load_default_yaml())

        self._base_runtime_config = parsed

        task_dict = parsed.get("task", {}) if isinstance(parsed.get("task", {}), dict) else {}
        task_dict.pop("id_column", None)
        self._task_extra_config = {
            k: v
            for k, v in task_dict.items()
            if k not in {"prompt_template", "target_schema", "task_name", "id_column"}
        }
        
        # 新增：分别加载prompt_template和target_schema
        prompt_template = task_dict.get("prompt_template", "")
        self._current_schema = task_dict.get("target_schema", {}).copy() if isinstance(task_dict.get("target_schema", {}), dict) else {}
        
        # 设置prompt_edit内容
        self.prompt_edit.setPlainText(prompt_template if prompt_template else "")
        
        # 更新schema显示标签
        self._update_schema_display_label()
        self._refresh_template_var_selector()

        llm_cfg = parsed.get("llm", {}) if isinstance(parsed.get("llm", {}), dict) else {}
        run_cfg = parsed.get("run", {}) if isinstance(parsed.get("run", {}), dict) else {}
        configured_task_name = str(task_dict.get("task_name", "") or "")

        # 设置并发、温度、超时的值（输入框）
        try:
            self.worker_spin.setValue(int(run_cfg.get("max_workers", 10) or 10))
        except (ValueError, TypeError):
            self.worker_spin.setValue(10)

        try:
            self.temperature_spin.setValue(float(llm_cfg.get("temperature", 0.0) or 0.0))
        except (ValueError, TypeError):
            self.temperature_spin.setValue(0.0)

        try:
            self.timeout_spin.setValue(int(llm_cfg.get("timeout", 60) or 60))
        except (ValueError, TypeError):
            self.timeout_spin.setValue(60)

        # 设置 think 模式
        enable_think = bool(llm_cfg.get("enable_think", False))
        self.enable_think_checkbox.setChecked(enable_think)

        self.refresh_model_profiles()
        self._set_model_selector_from_llm(llm_cfg)

        self.max_tokens_spin.setValue(int(llm_cfg.get("max_tokens", 2048) or 2048))

        if configured_task_name:
            self.task_name_edit.setText(configured_task_name)
        else:
            self.task_name_edit.setText("")

    def _extract_task_dict(self) -> dict:
        """从GUI组件提取task字典（prompt和schema）"""
        task = dict(self._task_extra_config)
        
        # 设置提示词模板
        prompt_text = self.prompt_edit.toPlainText().strip()
        if prompt_text:
            task["prompt_template"] = prompt_text
        
        # 设置目标列名（schema）
        if self._current_schema:
            task["target_schema"] = self._current_schema.copy()
        
        return task

    def _insert_text_to_prompt(self, text: str):
        """在提示词编辑器光标处插入文本"""
        if not text:
            return
        cursor = self.prompt_edit.textCursor()
        cursor.insertText(text)
        self.prompt_edit.setFocus()

    def _insert_selected_template_var(self):
        """插入当前选中的模板变量（统一 row.get 方式）"""
        field_name = self.template_var_selector.currentText().strip()
        if not field_name:
            self._notify("插入失败", "请先选择一个变量", "warning")
            return
        self._insert_text_to_prompt("{{ row.get('" + field_name + "') }}")

    def _insert_schema_placeholder(self):
        """插入输出列名说明占位符；未配置时禁止插入"""
        if not self._current_schema:
            self._notify("无法插入", "请先配置输出列名", "warning")
            return
        self._insert_text_to_prompt("{{ schema }}")

    def _insert_if_block_template(self):
        """插入 if 语句模板，默认使用当前变量"""
        field_name = self.template_var_selector.currentText().strip()
        if not field_name:
            self._notify("插入失败", "请先加载数据并选择列名", "warning")
            return
        block = "{% if row.get('" + field_name + "') %}\n"
        block += "" + field_name + "：{{ row.get('" + field_name + "') }}\n"
        block += "{% endif %}"
        self._insert_text_to_prompt(block)

    def _refresh_template_var_selector(self):
        """刷新可插入模板变量列表（仅来自已加载数据列名）"""
        candidates = []

        for i in range(self.preview_table.rowCount()):
            item = self.preview_table.item(i, 0)
            if item is None:
                continue
            col_name = item.text().strip()
            if col_name:
                candidates.append(col_name)

        uniq_cols = []
        for name in candidates:
            if name not in uniq_cols:
                uniq_cols.append(name)

        self._available_template_vars = uniq_cols

        current = self.template_var_selector.currentText().strip() if hasattr(self, "template_var_selector") else ""
        self.template_var_selector.blockSignals(True)
        self.template_var_selector.clear()
        for item in self._available_template_vars:
            self.template_var_selector.addItem(item)

        has_data_fields = self.template_var_selector.count() > 0
        self.template_var_selector.setEnabled(has_data_fields)
        self.insert_var_button.setEnabled(has_data_fields)
        self.insert_if_block_button.setEnabled(has_data_fields)
        self.insert_schema_button.setEnabled(bool(self._current_schema))

        if not has_data_fields:
            self.template_var_selector.addItem("")

        if has_data_fields and current:
            self._set_combo_value(self.template_var_selector, current)
        self.template_var_selector.blockSignals(False)

    def _collect_data_columns(self) -> list[str]:
        cols = []
        for i in range(self.preview_table.rowCount()):
            item = self.preview_table.item(i, 0)
            if item is None:
                continue
            col_name = item.text().strip()
            if col_name and col_name not in cols:
                cols.append(col_name)
        return cols

    def _validate_prompt_template(self, show_notify: bool = False) -> bool:
        """模板语法+列名引用校验，结果展示在状态标签"""
        prompt_text = self.prompt_edit.toPlainText().strip()
        if not prompt_text:
            self.template_validation_label.setText("模板为空")
            self.template_validation_label.setStyleSheet("color: #DC2626; font-size: 11px;")
            if show_notify:
                self._notify("提示词为空", "请先输入提示词", "warning")
            return False

        try:
            Template(prompt_text)
        except Exception as e:
            self.template_validation_label.setText(f"语法错误: {str(e)}")
            self.template_validation_label.setStyleSheet("color: #DC2626; font-size: 11px;")
            if show_notify:
                self._notify("语法错误", str(e), "error")
            return False

        required_cols, optional_cols = analyze_template_requirements(prompt_text)
        data_cols = set(self._collect_data_columns())
        if not data_cols:
            self.template_validation_label.setText(
                f"语法通过，未加载数据（必填{len(required_cols)}）"
            )
            self.template_validation_label.setStyleSheet("color: #B45309; font-size: 11px;")
            if show_notify:
                self._notify("未加载数据", "已跳过列名校验", "warning")
            return True

        missing_required = [c for c in required_cols if c not in data_cols]
        missing_optional = [c for c in optional_cols if c not in data_cols]

        if missing_required:
            self.template_validation_label.setText(
                f"缺少列名: 必填{missing_required}"
            )
            self.template_validation_label.setStyleSheet("color: #DC2626; font-size: 11px;")
            if show_notify:
                self._notify("列名缺失", f"必填: {missing_required}", "error")
            return False

        self.template_validation_label.setText(
            f"列名匹配通过（必填{len(required_cols)}）"
        )
        self.template_validation_label.setStyleSheet("color: #16A34A; font-size: 11px;")
        if show_notify:
            self._notify("校验通过", "可直接运行", "success")
        return True

    def _collect_preview_row_data(self) -> dict:
        """从数据预览表收集一行示例数据用于模板预览"""
        row_data = {}
        for i in range(self.preview_table.rowCount()):
            col_item = self.preview_table.item(i, 0)
            sample_item = self.preview_table.item(i, 4)
            if col_item is None:
                continue
            col_name = col_item.text().strip()
            if not col_name:
                continue
            sample_value = sample_item.text() if sample_item is not None else ""
            row_data[col_name] = sample_value
        return row_data

    def _render_full_prompt_preview(self) -> str:
        """渲染完整提示词（使用输出列名说明和示例row）"""
        task = self._extract_task_dict()
        prompt_template = task.get("prompt_template", "") if isinstance(task, dict) else ""
        if not prompt_template.strip():
            raise ValueError("提示词为空")

        schema_dict = task.get("target_schema", {}) if isinstance(task.get("target_schema", {}), dict) else {}
        schema_text = json.dumps(schema_dict, ensure_ascii=False, indent=2)
        row_data = self._collect_preview_row_data()

        template = Template(prompt_template)
        return template.render(row=row_data, schema=schema_text)

    def _preview_full_prompt(self):
        """弹窗预览完整提示词"""
        if not self._validate_prompt_template(show_notify=True):
            return
        try:
            rendered = self._render_full_prompt_preview()
        except Exception as e:
            self._notify("预览失败", str(e), "error")
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("提示词预览")
        parent_screen = self.screen() or QGuiApplication.primaryScreen()
        if parent_screen is not None:
            available = parent_screen.availableGeometry()
            dialog.resize(min(860, int(available.width() * 0.85)), min(620, int(available.height() * 0.85)))
        else:
            dialog.resize(860, 620)

        layout = QVBoxLayout(dialog)
        tip = QLabel("以下为当前提示词预览")
        tip.setWordWrap(True)
        layout.addWidget(tip)

        preview_edit = PlainTextEdit()
        preview_edit.setReadOnly(True)
        preview_edit.setPlainText(rendered)
        layout.addWidget(preview_edit, 1)

        action_row = QHBoxLayout()
        close_btn = PrimaryPushButton("关闭")
        close_btn.clicked.connect(dialog.accept)
        action_row.addStretch(1)
        action_row.addWidget(close_btn)
        layout.addLayout(action_row)

        dialog.exec()

    def _set_model_selector_from_llm(self, llm_cfg: dict):
        if self.model_selector.count() == 0:
            return

        target_model = str(llm_cfg.get("model", "") or "").strip()
        target_url = str(llm_cfg.get("base_url", "") or "").strip()

        if target_model:
            for r in self._model_records:
                model_name = str(r.get("model", "") or "").strip()
                base_url = str(r.get("base_url", "") or "").strip()
                if model_name == target_model and (not target_url or base_url == target_url):
                    self._set_combo_value(self.model_selector, str(r.get("name", "")))
                    return

        default_model = get_default_model()
        if default_model:
            self._set_combo_value(self.model_selector, default_model.get("name", "默认模型"))

    def _compose_runtime_config(self) -> dict:
        composed = dict(self._base_runtime_config)
        composed["llm"] = dict(composed.get("llm", {}))
        composed["run"] = dict(composed.get("run", {}))

        composed["task"] = self._extract_task_dict()
        composed["task"].pop("id_column", None)
        task_name = self.task_name_edit.text().strip()
        if task_name:
            composed["task"]["task_name"] = task_name
        else:
            composed["task"].pop("task_name", None)

        selected_model_name = self.model_selector.currentText().strip()
        model_profile = get_model_by_name(selected_model_name)
        if model_profile is None:
            model_profile = get_default_model()

        if model_profile:
            composed["llm"]["api_key"] = model_profile.get("api_key", composed["llm"].get("api_key", ""))
            composed["llm"]["base_url"] = model_profile.get("base_url", composed["llm"].get("base_url", ""))
            composed["llm"]["model"] = model_profile.get("model", composed["llm"].get("model", "glm-4-flash"))
            composed["llm"]["proxy_url"] = model_profile.get("proxy_url", "")

        try:
            composed["run"]["max_workers"] = int(self.worker_spin.value())
        except Exception:
            composed["run"]["max_workers"] = int(composed["run"].get("max_workers", 10) or 10)

        try:
            composed["llm"]["temperature"] = float(self.temperature_spin.value())
        except Exception:
            composed["llm"]["temperature"] = float(composed["llm"].get("temperature", 0.0) or 0.0)

        try:
            composed["llm"]["timeout"] = int(self.timeout_spin.value())
        except Exception:
            composed["llm"]["timeout"] = int(composed["llm"].get("timeout", 60) or 60)

        # 添加 think 模式配置
        composed["llm"]["enable_think"] = self.enable_think_checkbox.isChecked()

        composed["llm"]["max_tokens"] = int(self.max_tokens_spin.value() or 2048)
        composed.pop("settings", None)
        return composed

    def _refresh_line_numbers(self):
        """不再需要行号显示（prompt_edit已不使用行号视图）"""
        pass

    def _update_task_summary(self):
        """更新任务概览摘要"""
        task = self._extract_task_dict()
        schema = task.get("target_schema", {}) if isinstance(task.get("target_schema", {}), dict) else {}
        prompt = task.get("prompt_template", "") if isinstance(task.get("prompt_template", ""), str) else ""

        line_count = self.prompt_edit.toPlainText().count("\n") + 1
        schema_count = len(schema)
        prompt_len = len(prompt)
        summary = (
            f"任务: {self.task_name_edit.text().strip() or '自动命名'} | "
            f"列名: {schema_count} | 长度: {prompt_len} | "
            f"模型: {self.model_selector.currentText()} | 并发: {self.worker_spin.value()}"
        )
        if summary != self._task_summary_last:
            self.task_summary_label.setText(summary)
            self._task_summary_last = summary


    def _update_schema_display_label(self):
        """更新输出列名说明显示标签"""
        if self._current_schema:
            self.schema_display_label.setText(f"已配置 {len(self._current_schema)} 个列名")
        else:
            self.schema_display_label.setText("未配置")
        if hasattr(self, "insert_schema_button"):
            self.insert_schema_button.setEnabled(bool(self._current_schema))

    def _open_schema_editor(self):
        """打开输出列名说明编辑对话框"""
        dialog = SchemaEditorDialog(self._current_schema, self)
        if dialog.exec() == SchemaEditorDialog.DialogCode.Accepted:
            self._current_schema = dialog.get_schema()
            self._update_schema_display_label()
            self._schedule_config_save()
            self._update_task_summary()
            self.analyze_debouncer.call()

    def _export_config_file(self):
        """导出当前配置为YAML文件"""
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "导出配置",
            "task_config.yml",
            "YAML Files (*.yml *.yaml)",
        )
        if not file_path:
            return
        try:
            yaml_content = yaml.dump(self._compose_runtime_config(), Dumper=CustomYamlDumper, allow_unicode=True, sort_keys=False)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(yaml_content)
            self._notify("导出成功", Path(file_path).name, "success")
        except Exception as e:
            self._notify("导出失败", str(e), "error")

    def _import_config_file(self):
        """导入配置YAML文件"""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "导入配置",
            "",
            "YAML Files (*.yml *.yaml);;All Files (*)",
        )
        if not file_path:
            return
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                yaml_content = f.read()
            
            # 验证yaml格式
            parsed = self._safe_load_yaml(yaml_content)
            if not isinstance(parsed, dict):
                self._notify("导入失败", "配置格式不正确", "error")
                return

            # 新增：导入时同步写入模型库
            model_created_name = self._upsert_model_from_llm_config(parsed.get("llm", {}))
            
            # 应用配置
            self._apply_full_config_to_controls(yaml_content)
            self._update_task_summary()
            self.analyze_current_input()
            if model_created_name:
                self._notify("导入成功", f"已新建模型: {model_created_name}", "success")
            else:
                self._notify("导入成功", Path(file_path).name, "success")
            
        except Exception as e:
            self._notify("导入失败", str(e), "error")

    def _upsert_model_from_llm_config(self, llm_cfg: dict) -> str:
        """将导入的 llm 配置写入模型库；返回创建/命中的模型名"""
        if not isinstance(llm_cfg, dict):
            return ""

        api_key = str(llm_cfg.get("api_key", "") or "").strip()
        base_url = str(llm_cfg.get("base_url", "") or "").strip()
        model = str(llm_cfg.get("model", "") or "").strip()
        proxy_url = str(llm_cfg.get("proxy_url", "") or "").strip()

        if not api_key or not base_url or not model or "YOUR_API_KEY" in api_key:
            return ""

        for r in list_models():
            if str(r.get("model", "") or "").strip() == model and str(r.get("base_url", "") or "").strip() == base_url:
                self._set_combo_value(self.model_selector, str(r.get("name", "")))
                return str(r.get("name", ""))

        existed_names = {str(r.get("name", "") or "").strip() for r in list_models()}
        base_name = f"导入_{model}"
        model_name = base_name
        idx = 2
        while model_name in existed_names:
            model_name = f"{base_name}_{idx}"
            idx += 1

        ok = upsert_model(
            name=model_name,
            api_key=api_key,
            base_url=base_url,
            model=model,
            proxy_url=proxy_url,
            row_id="",
            make_default=False,
        )
        if not ok:
            return ""

        self.refresh_model_profiles()
        self._set_combo_value(self.model_selector, model_name)
        return model_name

    def _mask_key(self, key: str) -> str:
        if not key:
            return ""
        if len(key) <= 8:
            return "*" * len(key)
        return f"{key[:4]}***{key[-4:]}"

    def refresh_model_profiles(self):
        rows = list_models()
        self._model_records = rows

        # 工作台下拉
        current_name = self.model_selector.currentText().strip()
        self.model_selector.blockSignals(True)
        self.model_selector.clear()
        for r in rows:
            self.model_selector.addItem(r.get("name", ""))

        if current_name:
            self._set_combo_value(self.model_selector, current_name)
        elif rows:
            default_row = next((r for r in rows if int(r.get("is_default", 0)) == 1), rows[0])
            self._set_combo_value(self.model_selector, default_row.get("name", ""))
        self.model_selector.blockSignals(False)

        # 模型管理表格
        self.model_table.setRowCount(len(rows))
        for i, r in enumerate(rows):
            self.model_table.setItem(i, 0, QTableWidgetItem(r.get("name", "")))
            self.model_table.setItem(i, 1, QTableWidgetItem(r.get("model", "")))
            self.model_table.setItem(i, 2, QTableWidgetItem(r.get("base_url", "")))
            self.model_table.setItem(i, 3, QTableWidgetItem(r.get("proxy_url", "")))
            self.model_table.setItem(i, 4, QTableWidgetItem(self._mask_key(r.get("api_key", ""))))
            self.model_table.setItem(i, 5, QTableWidgetItem("是" if int(r.get("is_default", 0)) == 1 else ""))

    def _on_model_row_selected(self):
        row = self.model_table.currentRow()
        if row < 0 or row >= len(self._model_records):
            return

        item = self._model_records[row]
        self._selected_model_row_id = item.get("row_id", "")
        self.model_name_edit.setText(item.get("name", ""))
        self.model_name_raw_edit.setText(item.get("model", ""))
        self.model_url_edit.setText(item.get("base_url", ""))
        self.model_proxy_edit.setText(item.get("proxy_url", ""))
        self.model_key_edit.setText(item.get("api_key", ""))

    def _new_model_profile(self):
        self._selected_model_row_id = ""
        self.model_name_edit.clear()
        self.model_name_raw_edit.clear()
        self.model_url_edit.clear()
        self.model_proxy_edit.clear()
        self.model_key_edit.clear()

    def _save_model_profile(self, make_default: bool = False):
        name = self.model_name_edit.text().strip()
        model = self.model_name_raw_edit.text().strip()
        base_url = self.model_url_edit.text().strip()
        proxy_url = self.model_proxy_edit.text().strip()
        api_key = self.model_key_edit.text().strip()

        if not name or not model or not base_url or not api_key:
            self._notify("保存失败", "请填写完整模型信息", "warning")
            return

        ok = upsert_model(
            name=name,
            api_key=api_key,
            base_url=base_url,
            model=model,
            proxy_url=proxy_url,
            row_id=self._selected_model_row_id,
            make_default=make_default,
        )
        if not ok:
            self._notify("保存失败", "名称重复或输入有误", "error")
            return

        self.refresh_model_profiles()
        self._set_combo_value(self.model_selector, name)
        self._update_task_summary()
        self._schedule_config_save()
        self._notify("保存成功", name, "success")

    def _delete_model_profile(self):
        if not self._selected_model_row_id:
            self._notify("删除失败", "请先在模型列表选择一行", "warning")
            return

        if not delete_model(self._selected_model_row_id):
            self._notify("删除失败", "模型删除失败", "error")
            return

        self._new_model_profile()
        self.refresh_model_profiles()
        self._update_task_summary()
        self._schedule_config_save()
        self._notify("删除成功", "模型已删除", "success")

    def _apply_workspace_styles(self):
        self.setStyleSheet(
            """
            QWidget#workspacePage,
            QWidget#taskPage,
            QWidget#modelPage,
            QWidget#aboutPage {
                color: #1f2937;
            }
            QWidget#workspacePage {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #f4fbff, stop:0.48 #e8f1fa, stop:1 #dde8f3);
            }
            QWidget#taskPage {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #f7fbf6, stop:0.5 #edf6ef, stop:1 #e4efe8);
            }
            QWidget#modelPage {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #fdf9f3, stop:0.5 #f6efe2, stop:1 #f0e6d6);
            }
            QWidget#aboutPage {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #f8fbfc, stop:0.5 #eff3f6, stop:1 #e8edf2);
            }
            QLabel#workspaceIntroLabel {
                color: #143b63;
                background-color: rgba(255, 255, 255, 0.9);
                border: 1px solid rgba(20, 59, 99, 0.16);
                border-radius: 11px;
                padding: 8px 10px;
                font-size: 11px;
            }
            QLabel#pageHeroLabel {
                color: #314155;
                background-color: rgba(255, 255, 255, 0.85);
                border: 1px solid rgba(49, 65, 85, 0.14);
                border-radius: 11px;
                padding: 7px 10px;
                font-size: 11px;
                font-weight: 600;
            }
            CardWidget#workspaceCard {
                border-radius: 14px;
                border: 1px solid rgba(15, 23, 42, 0.1);
            }
            QWidget#workspaceCardContent {
                background-color: rgba(255, 255, 255, 0.98);
                border-bottom-left-radius: 14px;
                border-bottom-right-radius: 14px;
            }
            LineEdit, ComboBox, SpinBox, DoubleSpinBox, PlainTextEdit, TableWidget {
                border-radius: 10px;
                background-color: rgba(255, 255, 255, 0.95);
            }
            QWidget#workspacePage PushButton,
            QWidget#taskPage PushButton,
            QWidget#modelPage PushButton,
            QWidget#aboutPage PushButton {
                border-radius: 9px;
                padding: 2px 10px;
            }
            QWidget#workspacePage PrimaryPushButton,
            QWidget#taskPage PrimaryPushButton,
            QWidget#modelPage PrimaryPushButton,
            QWidget#aboutPage PrimaryPushButton {
                border-radius: 9px;
                padding: 2px 10px;
            }
            TableWidget {
                border: 1px solid rgba(30, 41, 59, 0.12);
            }
            QLabel#taskSummaryLabel {
                border: 1px solid rgba(15, 23, 42, 0.12);
                border-radius: 10px;
                background-color: rgba(15, 23, 42, 0.045);
                padding: 9px 11px;
                color: #1f3046;
            }
            QLabel#aboutTitleLabel {
                font-size: 21px;
                font-weight: 700;
                color: #12263d;
            }
            QLabel#aboutDescriptionLabel {
                color: #334155;
                font-size: 13px;
            }
            PlainTextEdit#lineNumberView {
                color: #64748b;
                background-color: rgba(15, 23, 42, 0.02);
                border: 1px solid rgba(15, 23, 42, 0.08);
                border-radius: 8px;
                font-size: 12px;
            }
            """
        )

    def _notify(self, title: str, content: str, level: str = "info"):
        fn = {
            "success": InfoBar.success,
            "warning": InfoBar.warning,
            "error": InfoBar.error,
            "info": InfoBar.info,
        }.get(level, InfoBar.info)
        fn(
            title=title,
            content=content,
            orient=Qt.Orientation.Horizontal,
            position=InfoBarPosition.TOP,
            isClosable=True,
            duration=2500,
            parent=self,
        )

    def _set_task_selector(self, selector_text: str):
        if not selector_text:
            return
        for i, item in enumerate(self._task_selectors):
            if item == selector_text:
                self.task_table.selectRow(i)
                return

    def _selected_task_selector(self) -> str:
        row = self.task_table.currentRow()
        if row < 0 or row >= len(self._task_selectors):
            return ""
        return self._task_selectors[row]

    def pick_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "选择数据文件",
            "",
            "Data Files (*.csv *.xlsx *.xls *.jsonl);;All Files (*)",
        )
        if file_path:
            self.file_path_edit.setText(file_path)

    def analyze_current_input(self):
        """
        关键优化：异步分析输入，避免UI阻塞
        在文件选择和配置变更时调用，不应阻塞UI线程
        """
        yaml_content = yaml.dump(self._compose_runtime_config(), Dumper=CustomYamlDumper, allow_unicode=True, sort_keys=False)
        file_path = self.file_path_edit.text().strip()

        # 提交异步任务，返回立即
        def do_analysis():
            return self.analysis_service.analyze(yaml_content, file_path)

        # 使用 execute_async 提交任务，结果通过 task_finished 信号返回
        self.async_runner.execute_async(do_analysis)

    def _on_analysis_finished(self, result):
        """异步分析完成的回调，在主线程执行"""
        if not result.get("ok"):
            err_type = result.get("error_type")
            self.preview_table.setRowCount(0)
            if err_type == "no-file":
                self.stats_label.setText("请选择有效数据文件")
            else:
                self.stats_label.setText(result.get("message", "配置或文件读取失败"))
            self.run_button.setEnabled(False)
            self._refresh_template_var_selector()
            self._validate_prompt_template(show_notify=False)
            return

        self.stats_label.setText(result.get("message", ""))
        self.run_button.setEnabled(bool(result.get("run_enabled")))

        preview = result.get("preview")
        if preview is None:
            self.preview_table.setRowCount(0)
            self._refresh_template_var_selector()
            self._validate_prompt_template(show_notify=False)
            return

        # 优化：使用快速表格填充
        TableOptimizer.fast_populate_table(self.preview_table, preview)
        self._refresh_template_var_selector()
        self._validate_prompt_template(show_notify=False)

    def _on_analysis_failed(self, error_msg: str):
        """异步分析失败的回调"""
        self.preview_table.setRowCount(0)
        self.stats_label.setText(f"分析失败: {error_msg}")
        self.run_button.setEnabled(False)
        self._refresh_template_var_selector()
        self._validate_prompt_template(show_notify=False)

    def _schedule_config_save(self):
        self.config_save_timer.start()

    def _persist_yaml_config(self):
        yaml_content = yaml.dump(self._compose_runtime_config(), Dumper=CustomYamlDumper, allow_unicode=True, sort_keys=False)
        save_yaml(yaml_content)

    def refresh_tasks(self):
        selected_selector = self._selected_task_selector()
        df = self.task_service.get_list_dataframe()
        self._task_selectors = self.task_service.get_selector_choices()

        # 优化：使用快速表格填充
        TableOptimizer.fast_populate_table(self.task_table, df)

        if selected_selector:
            self._set_task_selector(selected_selector)
        elif self.task_table.rowCount() > 0:
            self.task_table.selectRow(0)

    def load_selected_task(self):
        selector = self._selected_task_selector().strip()
        if not selector:
            self.task_action_label.setText("请先选任务")
            self._notify("未选任务", "请先选一条任务", "warning")
            return
        yaml_cfg, file_path = self.task_service.load_config(selector)
        if not yaml_cfg or not file_path:
            self.task_action_label.setText("任务不可用")
            self._notify("加载失败", "任务不可用", "warning")
            return

        self._apply_full_config_to_controls(yaml_cfg)
        self.file_path_edit.setText(file_path)
        self.task_action_label.setText(f"已加载: {selector}")
        self._notify("加载成功", selector, "success")
        self._update_task_summary()
        self.analyze_current_input()

    def delete_selected_task(self):
        selector = self._selected_task_selector().strip()
        if not selector:
            self.task_action_label.setText("请先选任务")
            self._notify("未选任务", "请先选一条任务", "warning")
            return
        result = self.task_service.delete_task(
            selector,
            self.runtime_service.active_task_hash(),
            self.runtime_service.is_running(),
        )
        self.task_action_label.setText(result)
        notify_level = "success" if result.startswith("✅") else "error"
        self._notify("任务删除", result, notify_level)
        self.refresh_tasks()

    def clear_selected_task(self):
        selector = self._selected_task_selector().strip()
        if not selector:
            self.task_action_label.setText("请先选任务")
            self._notify("未选任务", "请先选一条任务", "warning")
            return
        result = self.task_service.clear_results(
            selector,
            self.runtime_service.active_task_hash(),
            self.runtime_service.is_running(),
        )
        self.task_action_label.setText(result)
        self._notify("清空结果", result, "info")
        self.refresh_tasks()

    def start_runtime(self):
        if self.worker and self.worker.isRunning():
            self._notify("正在运行", "请先停止当前任务", "warning")
            return

        # 关键优化: 快速更新UI，不阻塞构建任务配置
        self.run_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.status_label.setText("⏳ 正在初始化...")
        self.progress_bar.setValue(0)
        
        yaml_content = yaml.dump(self._compose_runtime_config(), Dumper=CustomYamlDumper, allow_unicode=True, sort_keys=False)
        file_path = self.file_path_edit.text().strip()

        self.worker = RuntimeWorker(self.runtime_service, yaml_content, file_path, self)
        # 连接初始化信号，获得最快的视觉反馈
        self.worker.initializing.connect(self._on_worker_initializing)
        self.worker.event_emitted.connect(self._on_runtime_event)
        self.worker.failed.connect(self._on_runtime_failed)
        self.worker.start()

    def _on_worker_initializing(self):
        """Worker 线程启动信号，提供最快的视觉反馈"""
        self.status_label.setText("📊 加载数据中...")

    def stop_runtime(self):
        # 关键优化：快速禁用按钮，立即反馈意图
        self.stop_button.setEnabled(False)
        self.status_label.setText("🛑 正在安全中断...")
        
        # 后台触发停止请求，不阻塞UI
        def do_stop():
            result = self.runtime_service.trigger_stop()
            # 仅在需要时更新日志
            if result.get("log"):
                lines = result.get("log", "").split('\n')
                display_log = '\n'.join(lines[-30:])  # 只显示最后30行
                self.log_text.setPlainText(display_log)
        
        # 使用后台线程运行
        from threading import Thread
        stop_thread = Thread(target=do_stop, daemon=True)
        stop_thread.start()

    def _on_runtime_event(self, event: dict):
        event_type = event.get("type", "")
        status = event.get("status", "")
        log = event.get("log", "")
        shared = event.get("shared", {})

        # 只在状态实际改变时更新（去重，避免频繁UI更新）
        if status and status != self._last_poll_state['status']:
            self.status_label.setText(status)
            self._last_poll_state['status'] = status
        
        # 优化：只显示最后 50 行日志，避免大文本渲染卡顿
        if log and log != self._last_poll_state['log']:
            lines = log.split('\n')
            display_log = '\n'.join(lines[-50:])  # 只显示最后 50 行
            self.log_text.setPlainText(display_log)
            self._last_poll_state['log'] = log

        processed = max(int(shared.get("processed", 0) or 0), 0)
        total = max(int(shared.get("total", 0) or 0), 0)
        percent = 0 if total <= 0 else int(min(100, processed * 100 / total))
        
        # 只在进度实际改变时更新
        if percent != self._last_poll_state['percent']:
            self.progress_bar.setValue(percent)
            self._last_poll_state['percent'] = percent

        if event_type in ("finished", "error", "invalid-input"):
            self.stop_button.setEnabled(False)
            self.run_button.setEnabled(True)
            self.refresh_tasks()

    def _on_runtime_failed(self, message: str):
        self._notify("运行异常", message, "error")
        self.stop_button.setEnabled(False)
        self.run_button.setEnabled(True)
        
        # 优化：缓存上次的状态，避免重复更新UI
        if not hasattr(self, '_last_poll_state'):
            self._last_poll_state = {
                'percent': None,
                'log': None,
                'status': None,
                'running': None
            }

    def poll_runtime(self):
        snap = self.runtime_service.snapshot()
        shared = snap.shared_state or {}

        processed = max(int(shared.get("processed", 0) or 0), 0)
        total = max(int(shared.get("total", 0) or 0), 0)
        percent = 0 if total <= 0 else int(min(100, processed * 100 / total))
        
        # 优化：只在进度改变时更新进度条
        if percent != self._last_poll_state['percent']:
            self.progress_bar.setValue(percent)
            self._last_poll_state['percent'] = percent

        # 优化：只显示最后 50 行日志，避免大文本渲染
        if snap.latest_log and snap.latest_log != self._last_poll_state['log']:
            lines = snap.latest_log.split('\n')
            display_log = '\n'.join(lines[-50:])  # 只显示最后 50 行
            self.log_text.setPlainText(display_log)
            self._last_poll_state['log'] = snap.latest_log

        # 优化：只在状态改变时更新状态标签
        if snap.status_text and snap.status_text != self._last_poll_state['status']:
            self.status_label.setText(snap.status_text)
            self._last_poll_state['status'] = snap.status_text

        # 优化：只在运行状态改变时更新按钮
        is_running = snap.running or snap.starting
        if is_running != self._last_poll_state['running']:
            if is_running:
                self.run_button.setEnabled(False)
                self.stop_button.setEnabled(True)
            else:
                self.run_button.setEnabled(True)
                self.stop_button.setEnabled(False)
            self._last_poll_state['running'] = is_running

    def generate_export(self):
        yaml_content = yaml.dump(self._compose_runtime_config(), Dumper=CustomYamlDumper, allow_unicode=True, sort_keys=False)
        file_path = self.file_path_edit.text().strip()
        fmt = self.export_format.currentText()

        export_file = export_results_to_file(yaml_content, file_path, fmt)
        if not export_file:
            self.export_path_label.setText("导出失败：未找到结果或文件不可用")
            self.open_export_button.setEnabled(False)
            self._notify("导出失败", "请确认任务已运行并产生结果", "warning")
            return

        self.latest_export_file = export_file
        self.export_path_label.setText(export_file)
        self.open_export_button.setEnabled(True)
        self._notify("导出成功", Path(export_file).name, "success")

    def open_export(self):
        if not hasattr(self, "latest_export_file"):
            return
        if not os.path.exists(self.latest_export_file):
            self._notify("文件不存在", "导出文件可能已被移动", "warning")
            return

        os.startfile(self.latest_export_file)
