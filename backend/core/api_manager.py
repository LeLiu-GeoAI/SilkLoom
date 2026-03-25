"""
LLM API 调用管理 - 优化重试策略、错误处理和日志记录
"""
import logging
import time
from typing import Callable, Optional
import requests

from backend.core.config_constants import (
    API_MAX_RETRIES,
    API_RETRY_DELAYS,
    API_TIMEOUT_DEFAULT,
)
from backend.core.exceptions import APIError

logger = logging.getLogger(__name__)


class APICallManager:
    """
    管理LLM API调用，提供：
    - 指数退避重试
    - 详细错误记录
    - 超时控制
    - 停止信号支持
    """

    def __init__(self, api_key: str, base_url: str, timeout: int = API_TIMEOUT_DEFAULT):
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout

    def call_llm(
        self,
        model: str,
        messages: list,
        temperature: float = 0.0,
        max_tokens: int = 2048,
        stop_event=None,
        on_retry: Optional[Callable[[int, str], None]] = None,
    ) -> str:
        """
        调用LLM API，使用指数退避重试

        Args:
            model: 模型名称
            messages: 消息列表
            temperature: 温度参数
            max_tokens: 最大令牌数
            stop_event: 停止事件
            on_retry: 重试回调函数

        Returns:
            LLM响应文本

        Raises:
            APIError: API调用失败
        """
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        last_error = None
        for attempt in range(API_MAX_RETRIES):
            # 检查停止信号
            if stop_event and stop_event.is_set():
                logger.info("API调用被停止信号中断")
                return ""

            try:
                logger.debug(f"第 {attempt + 1} 次API调用: {model}")
                response = requests.post(
                    self.base_url,
                    headers=headers,
                    json=payload,
                    timeout=self.timeout,
                )
                response.raise_for_status()

                result = response.json()["choices"][0]["message"]["content"]
                if attempt > 0:
                    logger.info(f"API调用成功（第{attempt + 1}次尝试）")
                return result

            except requests.exceptions.Timeout as e:
                last_error = f"请求超时({self.timeout}s)"
                logger.warning(f"[尝试 {attempt + 1}] 超时: {last_error}")

            except requests.exceptions.ConnectionError as e:
                last_error = f"连接失败: {str(e)}"
                logger.warning(f"[尝试 {attempt + 1}] 连接错误: {last_error}")

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if hasattr(e, 'response') else None
                if status_code == 429:  # 频率限制
                    last_error = "API配额限制，请稍后重试"
                    delay = API_RETRY_DELAYS[min(attempt, len(API_RETRY_DELAYS) - 1)]
                    logger.warning(f"[尝试 {attempt + 1}] 配额限制，等待 {delay}s")
                else:
                    last_error = f"HTTP {status_code}: {str(e)}"
                    logger.warning(f"[尝试 {attempt + 1}] HTTP错误: {last_error}")

            except Exception as e:
                last_error = str(e)
                logger.warning(f"[尝试 {attempt + 1}] 未知错误: {last_error}")

            # 如果是最后一次尝试，不再延迟
            if attempt == API_MAX_RETRIES - 1:
                break

            # 执行指数退避延迟
            delay = API_RETRY_DELAYS[min(attempt, len(API_RETRY_DELAYS) - 1)]
            logger.info(f"将在 {delay}s 后进行第 {attempt + 2} 次尝试...")
            if on_retry:
                on_retry(attempt + 1, f"将重试，延迟 {delay}s...")

            for _ in range(delay):
                if stop_event and stop_event.is_set():
                    logger.info("延迟期间收到停止信号")
                    return ""
                time.sleep(1)

        # 所有重试都失败
        error_msg = f"大模型 API 请求失败（共{API_MAX_RETRIES}次尝试）: {last_error}"
        logger.error(error_msg)
        raise APIError(error_msg, retry_count=API_MAX_RETRIES)


class APIKeyResolver:
    """
    API Key解析器 - 支持环境变量优先级
    """

    @staticmethod
    def resolve_api_key(config_api_key: str) -> str:
        """
        解析API Key，优先级：
        1. 环境变量 LLM_API_KEY
        2. 环境变量 OPENAI_API_KEY
        3. 配置文件中的 api_key
        4. 抛出异常

        Args:
            config_api_key: 配置文件中的API Key

        Returns:
            有效的API Key

        Raises:
            APIError: 未找到有效的API Key
        """
        import os

        # 尝试环境变量
        env_key = os.getenv("LLM_API_KEY", "").strip() or os.getenv("OPENAI_API_KEY", "").strip()
        if env_key:
            logger.info("使用环境变量 LLM_API_KEY")
            return env_key

        # 尝试配置文件
        cfg_key = (config_api_key or "").strip()
        if cfg_key and "YOUR_API_KEY" not in cfg_key:
            logger.info("使用配置文件中的 API Key")
            return cfg_key

        # 未找到
        error_msg = "未配置 API Key。请在模型管理里配置api_key，或者环境变量 LLM_API_KEY（或 OPENAI_API_KEY）中设置"
        logger.error(error_msg)
        raise APIError(error_msg)
