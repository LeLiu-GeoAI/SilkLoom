"""
自定义异常类定义，提供更细粒度的错误处理
"""


class SilkLoomException(Exception):
    """SilkLoom项目基础异常"""
    pass


class ConfigError(SilkLoomException):
    """配置解析或验证失败"""
    pass


class APIError(SilkLoomException):
    """LLM API调用失败"""
    
    def __init__(self, message: str, status_code: int = None, retry_count: int = 0):
        self.message = message
        self.status_code = status_code
        self.retry_count = retry_count
        super().__init__(message)


class FileError(SilkLoomException):
    """文件读写或访问失败"""
    
    def __init__(self, message: str, file_path: str = None):
        self.message = message
        self.file_path = file_path
        super().__init__(message)


class DatabaseError(SilkLoomException):
    """数据库操作失败"""
    pass


class ValidationError(SilkLoomException):
    """数据验证失败"""
    
    def __init__(self, message: str, field: str = None, details: list = None):
        self.message = message
        self.field = field
        self.details = details or []
        super().__init__(message)


class TaskError(SilkLoomException):
    """任务管理异常"""
    pass


class ParseError(SilkLoomException):
    """JSON或数据解析失败"""
    
    def __init__(self, message: str, raw_text: str = None):
        self.message = message
        self.raw_text = raw_text
        super().__init__(message)


class ResourceError(SilkLoomException):
    """资源不足（内存、线程等）"""
    pass
