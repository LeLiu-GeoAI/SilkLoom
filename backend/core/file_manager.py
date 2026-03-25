"""
文件处理优化 - 改进的文件读取、验证和错误处理
"""
import os
import time
import logging
from typing import Tuple, Optional, List

from backend.core.config_constants import (
    SUPPORTED_FILE_FORMATS,
    FILE_READ_RETRY_ATTEMPTS,
    FILE_READ_RETRY_DELAY,
)
from backend.core.exceptions import FileError, ValidationError
from backend.core.data_io import (
    DataTable,
    universal_read_data,
    write_records_csv,
    write_records_excel,
    write_records_jsonl,
)

logger = logging.getLogger(__name__)


class FileManager:
    """
    文件管理器 - 处理文件读取、验证和转换
    """

    @staticmethod
    def is_supported_format(file_path: str) -> bool:
        """
        检查文件格式是否支持

        Args:
            file_path: 文件路径

        Returns:
            是否支持
        """
        ext = os.path.splitext(file_path)[1].lower()
        return ext in SUPPORTED_FILE_FORMATS

    @staticmethod
    def read_data_with_retry(file_path: str) -> DataTable:
        """
        带重试的数据读取

        Args:
            file_path: 文件路径

        Returns:
            DataTable

        Raises:
            FileError: 文件读取失败
        """
        if not os.path.exists(file_path):
            logger.error(f"文件不存在: {file_path}")
            raise FileError(f"文件不存在: {file_path}", file_path)

        ext = os.path.splitext(file_path)[1].lower()
        if not FileManager.is_supported_format(file_path):
            logger.error(f"不支持的文件格式: {ext}")
            raise FileError(f"不支持的文件格式: {ext}", file_path)

        last_error = None
        for attempt in range(FILE_READ_RETRY_ATTEMPTS):
            try:
                return universal_read_data(file_path)
            except Exception as e:
                last_error = str(e)
                logger.warning(f"[尝试 {attempt + 1}] 读取失败: {last_error}")

            if attempt < FILE_READ_RETRY_ATTEMPTS - 1:
                logger.info(f"将在 {FILE_READ_RETRY_DELAY}s 后进行第 {attempt + 2} 次尝试...")
                time.sleep(FILE_READ_RETRY_DELAY)

        error_msg = f"无法读取文件（共{FILE_READ_RETRY_ATTEMPTS}次尝试）: {last_error}"
        logger.error(error_msg)
        raise FileError(error_msg, file_path)

    @staticmethod
    def validate_columns(df: DataTable, required_cols: List[str], optional_cols: List[str] = None) -> Tuple[bool, Optional[str]]:
        """
        验证表格中的列

        Args:
            df: 数据框
            required_cols: 必需的列
            optional_cols: 可选的列

        Returns:
            (是否有效, 错误消息)
        """
        optional_cols = optional_cols or []
        df_cols = set(df.columns)

        missing_required = [c for c in required_cols if c not in df_cols]
        if missing_required:
            error_msg = f"数据缺少必填列名: {missing_required}"
            logger.error(error_msg)
            return False, error_msg

        missing_optional = [c for c in optional_cols if c not in df_cols]
        if missing_optional:
            warning_msg = f"数据缺少可选列名: {missing_optional}"
            logger.warning(warning_msg)

        return True, None

    @staticmethod
    def get_file_info(file_path: str) -> dict:
        """
        获取文件信息

        Args:
            file_path: 文件路径

        Returns:
            文件信息字典
        """
        if not os.path.exists(file_path):
            raise FileError(f"文件不存在: {file_path}", file_path)

        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        file_mtime = os.path.getmtime(file_path)

        return {
            "path": file_path,
            "size_mb": round(file_size, 2),
            "mtime": file_mtime,
            "format": os.path.splitext(file_path)[1].lower(),
        }

    @staticmethod
    def save_dataframe(df: DataTable, output_path: str, format_type: str = "CSV") -> bool:
        """
        保存表格到文件

        Args:
            df: 数据框
            output_path: 输出路径
            format_type: 格式类型 (CSV, Excel, JSONL)

        Returns:
            是否成功
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            if format_type == "CSV":
                write_records_csv(df.rows, output_path)
            elif format_type == "Excel":
                write_records_excel(df.rows, output_path)
            elif format_type == "JSONL":
                write_records_jsonl(df.rows, output_path)
            else:
                raise ValueError(f"不支持的格式: {format_type}")

            logger.info(f"数据已保存到: {output_path}")
            return True

        except Exception as e:
            logger.error(f"保存文件失败: {e}")
            raise FileError(f"保存文件失败: {e}", output_path)
