"""
日志系统 - 集中管理日志配置和实现
"""
import logging
import logging.handlers
import os
from datetime import datetime

from backend.core.config_constants import LOG_DIR, LOG_FORMAT, LOG_DATE_FORMAT, LOG_LEVEL


class LoggerFactory:
    """
    日志工厂 - 创建和管理应用中的所有日志记录器
    """

    _initialized = False
    _loggers = {}

    @classmethod
    def initialize(cls):
        """初始化日志系统"""
        if cls._initialized:
            return

        # 配置根日志记录器
        root_logger = logging.getLogger()
        root_logger.setLevel(LOG_LEVEL)

        # 移除默认处理器
        root_logger.handlers.clear()

        # 添加控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(LOG_LEVEL)
        console_formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

        # 添加文件处理器
        log_file = os.path.join(LOG_DIR, f"silkloom_{datetime.now().strftime('%Y%m%d')}.log")
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(LOG_LEVEL)
        file_formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

        cls._initialized = True
        root_logger.info("日志系统初始化完成")

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        获取日志记录器

        Args:
            name: 记录器名称，通常使用 __name__

        Returns:
            配置好的Logger实例
        """
        if not cls._initialized:
            cls.initialize()

        if name not in cls._loggers:
            cls._loggers[name] = logging.getLogger(name)

        return cls._loggers[name]


def setup_logging():
    """应用启动时调用此函数"""
    LoggerFactory.initialize()
