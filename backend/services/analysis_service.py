import os
import time
from datetime import datetime

import yaml

from backend.core.data_io import universal_read_data
from backend.core.template_tools import analyze_template_requirements


def _infer_dtype(non_null_values):
    if not non_null_values:
        return "string"

    bool_like = True
    int_like = True
    float_like = True
    datetime_like = True

    for value in non_null_values:
        if not isinstance(value, bool):
            bool_like = False

        if not isinstance(value, int) or isinstance(value, bool):
            int_like = False

        if not isinstance(value, (int, float)) or isinstance(value, bool):
            float_like = False

        if not isinstance(value, datetime):
            datetime_like = False

    if bool_like:
        return "bool"
    if int_like:
        return "int"
    if float_like:
        return "float"
    if datetime_like:
        return "datetime"
    return "string"


class FileAnalysisService:
    def __init__(self):
        self._file_cache = {
            "path": "",
            "mtime": 0,
            "cols": set(),
            "total_rows": 0,
            "total_cols": 0,
            "summary_data": [],
        }

    def analyze(self, yaml_str, file_path):
        if not file_path or not os.path.exists(file_path):
            return {
                "ok": False,
                "message": "",
                "preview": None,
                "run_enabled": False,
                "error_type": "no-file",
            }

        req_cols, opt_cols = [], []
        try:
            config = yaml.safe_load(yaml_str)
            if not isinstance(config, dict):
                config = {}
            template_str = config.get("task", {}).get("prompt_template", "")
            req_cols, opt_cols = analyze_template_requirements(template_str)
        except Exception as e:
            return {
                "ok": False,
                "message": f"配置错误: {str(e)}",
                "preview": None,
                "run_enabled": False,
                "error_type": "yaml",
            }

        current_mtime = os.path.getmtime(file_path)
        if self._file_cache["path"] == file_path and self._file_cache["mtime"] == current_mtime:
            df_cols = self._file_cache["cols"]
            total_rows = self._file_cache["total_rows"]
            total_cols = self._file_cache["total_cols"]
            summary_data = self._file_cache["summary_data"]
        else:
            table = None
            for _ in range(10):
                try:
                    table = universal_read_data(file_path)
                    break
                except Exception:
                    time.sleep(0.5)
            if table is None:
                return {
                    "ok": False,
                    "message": "文件暂时不可读",
                    "preview": None,
                    "run_enabled": False,
                    "error_type": "read",
                }

            df_cols = set(table.columns)
            total_rows = len(table)
            total_cols = len(table.columns)

            summary_data = []
            for col in table.columns:
                col_values = [row.get(col) for row in table.rows]
                non_null_values = [v for v in col_values if v is not None and str(v).strip() != ""]
                non_null_count = len(non_null_values)
                missing_count = total_rows - non_null_count
                dtype_name = _infer_dtype(non_null_values)
                sample_value = ""

                if non_null_values:
                    raw_sample = str(non_null_values[0])
                    sample_value = raw_sample if len(raw_sample) <= 48 else f"{raw_sample[:45]}..."

                summary_data.append({
                    "列名": col,
                    "有效值": non_null_count,
                    "缺失值": missing_count,
                    "类型": dtype_name,
                    "示例值": sample_value,
                })

            self._file_cache = {
                "path": file_path,
                "mtime": current_mtime,
                "cols": df_cols,
                "total_rows": total_rows,
                "total_cols": total_cols,
                "summary_data": summary_data,
            }

        missing_req = [c for c in req_cols if c not in df_cols]
        missing_opt = [c for c in opt_cols if c not in df_cols]

        is_valid = True
        msg_parts = []

        def _join_cols(cols: list[str]) -> str:
            return "、".join(cols)

        if missing_req:
            msg_parts.append(f"缺少必填列名: {_join_cols(missing_req)}")
            is_valid = False
        if missing_opt:
            msg_parts.append(f"缺少可选列名: {_join_cols(missing_opt)}")

        if not missing_req and not missing_opt and (req_cols or opt_cols):
            msg_parts.append("列名匹配通过")

        msg_parts.append(f"数据: {total_rows} 行, {total_cols} 列")

        return {
            "ok": True,
            "message": "，".join(msg_parts),
            "preview": summary_data,
            "run_enabled": is_valid,
            "error_type": "",
        }
