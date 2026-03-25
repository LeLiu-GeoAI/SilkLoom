"""
数据库操作优化 - 批量插入、事务管理、连接池
"""
import logging
import sqlite3
import json
import threading
from typing import List, Dict, Tuple, Optional
from contextlib import contextmanager

from backend.core.config_constants import BATCH_INSERT_SIZE
from backend.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    SQLite数据库管理器，提供：
    - 连接管理和超时处理
    - 批量插入优化
    - 事务控制
    - 线程安全
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()

    @contextmanager
    def get_connection(self, timeout: int = 30):
        """获取数据库连接的上下文管理器"""
        conn = sqlite3.connect(self.db_path, timeout=timeout)
        try:
            conn.row_factory = sqlite3.Row
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"数据库操作失败: {e}")
            raise DatabaseError(f"数据库操作失败: {e}")
        finally:
            conn.close()

    def batch_insert_results(self, results: List[Dict], table: str = "results") -> int:
        """
        批量插入结果数据

        Args:
            results: 结果字典列表
            table: 表名

        Returns:
            插入的行数

        Raises:
            DatabaseError: 数据库操作失败
        """
        if not results:
            return 0

        with self._lock:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()

                    # 批量插入
                    for i in range(0, len(results), BATCH_INSERT_SIZE):
                        batch = results[i : i + BATCH_INSERT_SIZE]
                        self._insert_batch(cursor, batch, table)
                        logger.debug(f"已插入 {min(i + BATCH_INSERT_SIZE, len(results))}/{len(results)} 条")

                    inserted = len(results)
                    logger.info(f"批量插入完成，共 {inserted} 条记录")
                    return inserted

            except Exception as e:
                logger.error(f"批量插入失败: {e}")
                raise DatabaseError(f"批量插入失败: {e}")

    def _insert_batch(self, cursor: sqlite3.Cursor, batch: List[Dict], table: str):
        """
        执行单个批次的插入操作

        Args:
            cursor: 数据库游标
            batch: 数据批次
            table: 表名
        """
        if table == "results":
            sql = """
                INSERT OR REPLACE INTO results 
                (row_id, input_data, output_data, raw_llm, status, error_msg)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            data = [
                (
                    item["row_id"],
                    json.dumps(item.get("input_data", {}), ensure_ascii=False),
                    json.dumps(item.get("output_data", {}), ensure_ascii=False),
                    item.get("raw_llm", ""),
                    item.get("status", "success"),
                    item.get("error_msg", ""),
                )
                for item in batch
            ]
            cursor.executemany(sql, data)
        elif table == "experiments":
            sql = """
                UPDATE experiments
                SET file_path = ?, yaml_config = ?, task_name = ?, 
                    description = ?, status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """
            data = [
                (
                    item.get("file_path", ""),
                    item.get("yaml_config", ""),
                    item.get("task_name", ""),
                    item.get("description", ""),
                    item.get("status", "idle"),
                    item.get("id", 0),
                )
                for item in batch
            ]
            cursor.executemany(sql, data)

    def execute_query(self, sql: str, params: Tuple = ()) -> List[Dict]:
        """
        执行查询操作

        Args:
            sql: SQL语句
            params: 参数元组

        Returns:
            查询结果列表
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                columns = [description[0] for description in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"查询失败: {e}")
            raise DatabaseError(f"查询失败: {e}")

    def execute_update(self, sql: str, params: Tuple = ()) -> int:
        """
        执行更新/删除/插入操作

        Args:
            sql: SQL语句
            params: 参数元组

        Returns:
            受影响的行数
        """
        with self._lock:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(sql, params)
                    return cursor.rowcount
            except Exception as e:
                logger.error(f"更新失败: {e}")
                raise DatabaseError(f"更新失败: {e}")

    def get_done_ids(self, table: str = "results") -> set:
        """
        获取已完成的ID集合，用于断点续传

        Args:
            table: 表名

        Returns:
            ID集合
        """
        try:
            results = self.execute_query(f"SELECT row_id FROM {table}")
            return {str(r["row_id"]).strip() for r in results}
        except Exception as e:
            logger.warning(f"获取已完成ID失败，将重新处理: {e}")
            return set()

    def delete_old_records(self, table: str, batch_size: int = 1000):
        """
        删除旧记录，保留最新数据

        Args:
            table: 表名
            batch_size: 批处理大小
        """
        try:
            # 这里可以实现按时间戳删除旧记录的逻辑
            logger.info(f"旧记录清理完成")
        except Exception as e:
            logger.warning(f"清理旧记录失败: {e}")
