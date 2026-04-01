import argparse
import base64
import csv
import json
import mimetypes
import os
from pathlib import Path
from typing import List, Dict, Any

import requests
import yaml


def encode_data_url(image_path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(image_path))
    if not mime:
        mime = "application/octet-stream"
    with image_path.open("rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"


def resolve_api_key(llm_cfg: Dict[str, Any]) -> str:
    env_key = (
        os.getenv("ZAI_API_KEY", "").strip()
        or os.getenv("LLM_API_KEY", "").strip()
        or os.getenv("OPENAI_API_KEY", "").strip()
    )
    if env_key:
        return env_key

    cfg_key = str(llm_cfg.get("api_key", "") or "").strip()
    if cfg_key and "xxxx" not in cfg_key.lower() and "your_api_key" not in cfg_key.lower():
        return cfg_key

    raise RuntimeError("未找到有效 API Key。请设置环境变量 ZAI_API_KEY/LLM_API_KEY。")


def build_messages(prompt: str, image_items: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    content: List[Dict[str, Any]] = [{"type": "text", "text": prompt}]
    for idx, item in enumerate(image_items, start=1):
        label = (item.get("label") or "").strip()
        image_url = item["url"]
        content.append({"type": "text", "text": label if label else f"图片{idx}"})
        content.append({"type": "image_url", "image_url": {"url": image_url}})
    return [{"role": "user", "content": content}]


def call_chat(api_key: str, base_url: str, model: str, messages: List[Dict[str, Any]], timeout: int = 90) -> requests.Response:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": 1024,
    }
    return requests.post(base_url, headers=headers, json=payload, timeout=timeout)


def load_first_row_images(data_csv: Path) -> List[Dict[str, str]]:
    with data_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        row = next(reader, None)
    if not row:
        return []

    raw = (row.get("image_path") or "").strip()
    if not raw:
        return []

    items = json.loads(raw)
    if not isinstance(items, list):
        return []

    result: List[Dict[str, str]] = []
    base_dir = data_csv.resolve().parent
    for it in items:
        if not isinstance(it, dict):
            continue
        rel_path = str(it.get("path") or "").strip()
        if not rel_path:
            continue
        p = Path(rel_path)
        if not p.is_absolute():
            p = (base_dir / p).resolve()
        if not p.exists():
            print(f"[WARN] 文件不存在: {p}")
            continue
        result.append({
            "label": str(it.get("label") or "").strip(),
            "url": encode_data_url(p),
            "path": str(p),
        })
    return result


def print_result(case_name: str, resp: requests.Response):
    print("=" * 80)
    print(f"CASE: {case_name}")
    print(f"HTTP: {resp.status_code}")
    try:
        body = resp.json()
        print(json.dumps(body, ensure_ascii=False, indent=2)[:5000])
    except Exception:
        print(resp.text[:5000])


def is_image_limit_error(resp: requests.Response) -> bool:
    if resp.status_code != 400:
        return False
    try:
        body = resp.json()
    except Exception:
        return "输入图片数量超过限制" in (resp.text or "")
    msg = str((body.get("error") or {}).get("message") or "")
    return "输入图片数量超过限制" in msg


def find_max_supported_images(api_key: str, base_url: str, model: str, prompt: str, items: List[Dict[str, str]]) -> int:
    lo, hi = 1, len(items)
    best = 0
    while lo <= hi:
        mid = (lo + hi) // 2
        resp = call_chat(api_key, base_url, model, build_messages(prompt, items[:mid]))
        if resp.status_code == 200:
            best = mid
            lo = mid + 1
        elif is_image_limit_error(resp):
            hi = mid - 1
        else:
            # 非图片数量超限错误，停止探测
            break
    return best


def main():
    parser = argparse.ArgumentParser(description="SilkLoom VLM 多图参数诊断脚本")
    parser.add_argument(
        "--config",
        default=str(Path("examples") / "05 图像变化" / "task_config.yml"),
        help="任务配置文件路径",
    )
    parser.add_argument(
        "--data",
        default=str(Path("examples") / "05 图像变化" / "data.csv"),
        help="数据文件路径",
    )
    parser.add_argument(
        "--take",
        type=int,
        default=3,
        help="多图测试数量（默认3）",
    )
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    data_path = Path(args.data).resolve()

    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    llm_cfg = cfg.get("llm", {}) if isinstance(cfg.get("llm", {}), dict) else {}

    api_key = resolve_api_key(llm_cfg)
    base_url = str(llm_cfg.get("base_url", "")).strip()
    model = str(llm_cfg.get("model", "")).strip()
    if not base_url or not model:
        raise RuntimeError("配置缺少 llm.base_url 或 llm.model")

    prompt = (
        "这些是同一地点不同时期的街景，请判断建筑物是否有变化，并指出具体变化，"
        "按时间顺序分析，仅输出简洁结论。"
    )

    items = load_first_row_images(data_path)
    if not items:
        raise RuntimeError("未从 data.csv 第一行解析到任何图片")

    # Case A: 单图（基线）
    msg_single = build_messages(prompt, items[:1])
    resp_single = call_chat(api_key, base_url, model, msg_single)
    print_result("single-image", resp_single)

    # Case B: 多图（默认取前 N 张）
    n = max(2, min(args.take, len(items)))
    msg_multi_n = build_messages(prompt, items[:n])
    resp_multi_n = call_chat(api_key, base_url, model, msg_multi_n)
    print_result(f"multi-image-{n}", resp_multi_n)

    # Case C: 全部图片
    msg_multi_all = build_messages(prompt, items)
    resp_multi_all = call_chat(api_key, base_url, model, msg_multi_all)
    print_result(f"multi-image-all-{len(items)}", resp_multi_all)

    if is_image_limit_error(resp_multi_all):
        max_ok = find_max_supported_images(api_key, base_url, model, prompt, items)
        print("=" * 80)
        print(f"DETECTED LIMIT: 单次最多可通过图片数约为 {max_ok} 张")

    print("=" * 80)
    print("DONE")


if __name__ == "__main__":
    main()
