"""Schema Editor Dialog - GUI for configuring target_schema"""

from PySide6.QtCore import Qt
from PySide6.QtGui import QGuiApplication
from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QTableWidget,
    QTableWidgetItem,
    QLabel,
    QHeaderView,
    QAbstractItemView,
)
from typing import Optional
from qfluentwidgets import (
    PrimaryPushButton,
    PushButton,
    LineEdit,
    InfoBar,
    InfoBarPosition,
)


class SchemaEditorDialog(QDialog):
    """Dialog for editing task target_schema (field definitions)"""
    
    def __init__(self, schema_dict: Optional[dict] = None, parent=None):
        super().__init__(parent)
        self.schema_dict = schema_dict.copy() if schema_dict else {}
        self._init_ui()
        self._load_schema_to_table()
        
    def _init_ui(self):
        """Initialize the dialog UI"""
        self.setWindowTitle("配置输出列名")
        screen = self.parent().screen() if self.parent() is not None else QGuiApplication.primaryScreen()
        if screen is not None:
            available = screen.availableGeometry()
            self.resize(min(700, int(available.width() * 0.8)), min(500, int(available.height() * 0.8)))
        else:
            self.resize(700, 500)
        
        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Title
        title = QLabel("配置输出列名")
        title.setStyleSheet("font-size: 14px; font-weight: bold;")
        layout.addWidget(title)
        
        # Description
        desc = QLabel("添加列名和说明")
        desc.setWordWrap(True)
        desc.setStyleSheet("color: #666; font-size: 12px;")
        layout.addWidget(desc)
        
        # Table for schema fields
        self.schema_table = QTableWidget()
        self.schema_table.setColumnCount(2)
        self.schema_table.setHorizontalHeaderLabels(["列名", "说明"])
        self.schema_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.schema_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.schema_table.setEditTriggers(QAbstractItemView.EditTrigger.DoubleClicked)
        
        header = self.schema_table.horizontalHeader()
        if header is not None:
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        
        self.schema_table.setMinimumHeight(300)
        layout.addWidget(self.schema_table)
        
        # Action buttons row 1: Add/Delete
        action_row_1 = QHBoxLayout()
        self.add_field_button = PrimaryPushButton("添加列名")
        self.delete_field_button = PushButton("删除列名")
        self.add_field_button.clicked.connect(self._add_field)
        self.delete_field_button.clicked.connect(self._delete_field)
        action_row_1.addWidget(self.add_field_button)
        action_row_1.addWidget(self.delete_field_button)
        action_row_1.addStretch()
        layout.addLayout(action_row_1)
        
        # Action buttons row 2: OK/Cancel
        action_row_2 = QHBoxLayout()
        self.ok_button = PrimaryPushButton("保存配置")
        self.cancel_button = PushButton("取消")
        self.ok_button.clicked.connect(self._save_and_close)
        self.cancel_button.clicked.connect(self.reject)
        action_row_2.addStretch()
        action_row_2.addWidget(self.ok_button)
        action_row_2.addWidget(self.cancel_button)
        layout.addLayout(action_row_2)
    
    def _load_schema_to_table(self):
        """Load schema_dict into the table"""
        self.schema_table.setRowCount(len(self.schema_dict))
        for row, (field_name, field_desc) in enumerate(self.schema_dict.items()):
            name_item = QTableWidgetItem(field_name)
            desc_item = QTableWidgetItem(str(field_desc))
            self.schema_table.setItem(row, 0, name_item)
            self.schema_table.setItem(row, 1, desc_item)
    
    def _add_field(self):
        """Add a new empty field row"""
        row_count = self.schema_table.rowCount()
        self.schema_table.insertRow(row_count)
        
        # Auto-set default field name
        default_name = f"列名{row_count + 1}"
        name_item = QTableWidgetItem(default_name)
        desc_item = QTableWidgetItem("说明")
        
        self.schema_table.setItem(row_count, 0, name_item)
        self.schema_table.setItem(row_count, 1, desc_item)
        
        # Select the newly added row
        self.schema_table.selectRow(row_count)
    
    def _delete_field(self):
        """Delete the selected field row"""
        row = self.schema_table.currentRow()
        if row >= 0:
            self.schema_table.removeRow(row)
    
    def _save_and_close(self):
        """Save schema from table and close dialog"""
        # Validate that all fields have names
        for row in range(self.schema_table.rowCount()):
            field_name = self.schema_table.item(row, 0)
            if not field_name or not field_name.text().strip():
                InfoBar.warning(
                    title="列名不能为空",
                    content=f"第 {row + 1} 行列名为空",
                    orient=Qt.Orientation.Vertical,
                    isClosable=True,
                    position=InfoBarPosition.TOP,
                    parent=self,
                )
                return
        
        # Extract schema from table
        self.schema_dict = {}
        for row in range(self.schema_table.rowCount()):
            name_item = self.schema_table.item(row, 0)
            desc_item = self.schema_table.item(row, 1)
            field_name = name_item.text().strip() if name_item else ""
            field_desc = desc_item.text().strip() if desc_item else ""
            if not field_name:
                continue
            self.schema_dict[field_name] = field_desc
        
        self.accept()
    
    def get_schema(self) -> dict:
        """Return the edited schema dictionary"""
        return self.schema_dict
