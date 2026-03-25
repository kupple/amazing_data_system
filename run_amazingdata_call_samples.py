#!/usr/bin/env python3
"""
读取 AMAZINGDATA_CALL_API_SAMPLES.md 中的接口样例，
逐个请求 AmazingData call API，并保存失败信息。
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from urllib import error, request


DEFAULT_URL = "http://100.93.115.99:8000/api/amazingdata/call"
DEFAULT_DOC = Path(__file__).resolve().parent / "AMAZINGDATA_CALL_API_SAMPLES.md"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "tmp" / "amazingdata_call_reports"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="读取 AmazingData call API 样例文档并逐个发起 POST 测试。"
    )
    parser.add_argument(
        "--doc",
        default=str(DEFAULT_DOC),
        help="样例文档路径，默认读取 AMAZINGDATA_CALL_API_SAMPLES.md",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="AmazingData call API 地址",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="单个请求超时时间，单位秒",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="结果输出目录",
    )
    parser.add_argument(
        "--failed-json",
        default="",
        help="只重跑失败接口时使用，传上一次生成的 amazingdata_call_errors_*.json 路径",
    )
    return parser.parse_args()


def extract_cases(markdown_text: str) -> List[Dict[str, Any]]:
    """
    从文档中提取方法样例。

    仅提取形如:
    ### 1.1 get_code_list
    ```json
    ...
    ```
    的代码块。
    """
    pattern = re.compile(
        r"^###\s+\d+\.\d+\s+([A-Za-z0-9_\.]+)\s*$\n+```json\n(.*?)\n```",
        re.MULTILINE | re.DOTALL,
    )

    cases: List[Dict[str, Any]] = []
    seen_methods = set()

    for match in pattern.finditer(markdown_text):
        title_method = match.group(1).strip()
        payload_text = match.group(2).strip()

        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError as exc:
            cases.append(
                {
                    "title_method": title_method,
                    "method": title_method,
                    "payload": None,
                    "parse_error": f"JSON 解析失败: {exc}",
                }
            )
            continue

        method = payload.get("method", title_method)
        if method in seen_methods:
            continue
        seen_methods.add(method)

        cases.append(
            {
                "title_method": title_method,
                "method": method,
                "payload": payload,
                "parse_error": None,
            }
        )

    return cases


def load_failed_cases(failed_json_path: Path) -> List[Dict[str, Any]]:
    """从失败报告中提取需要重跑的请求体。"""
    obj = json.loads(failed_json_path.read_text(encoding="utf-8"))
    failed_results = obj.get("failed_results", [])
    cases: List[Dict[str, Any]] = []

    for item in failed_results:
        payload = item.get("request_payload")
        method = item.get("method")
        title_method = item.get("title_method", method)
        if not payload or not method:
            continue
        cases.append(
            {
                "title_method": title_method,
                "method": method,
                "payload": payload,
                "parse_error": None,
            }
        )

    return cases


def extract_error_message(status_code: int, response_text: str, response_json: Any) -> str:
    if isinstance(response_json, dict):
        data = response_json.get("data")
        if isinstance(data, dict) and data.get("error"):
            return str(data["error"])
        detail = response_json.get("detail")
        if detail:
            return str(detail)
        message = response_json.get("message")
        if message and status_code != 200:
            return str(message)
    text = response_text.strip()
    return text or f"HTTP {status_code}"


def run_cases(
    cases: List[Dict[str, Any]],
    url: str,
    timeout: float,
) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    started_at = datetime.now().isoformat()

    for index, case in enumerate(cases, start=1):
        method = case["method"]
        payload = case["payload"]

        if case["parse_error"]:
            results.append(
                {
                    "index": index,
                    "method": method,
                    "title_method": case["title_method"],
                    "ok": False,
                    "status_code": None,
                    "error": case["parse_error"],
                    "request_payload": None,
                    "response_json": None,
                    "response_text": None,
                }
            )
            continue

        request_body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = request.Request(
            url=url,
            data=request_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=timeout) as response:
                status_code = response.getcode()
                response_text = response.read().decode("utf-8", errors="replace")
                try:
                    response_json = json.loads(response_text)
                except Exception:
                    response_json = None

            ok = status_code == 200
            error_message = None if ok else extract_error_message(status_code, response_text, response_json)

            results.append(
                {
                    "index": index,
                    "method": method,
                    "title_method": case["title_method"],
                    "ok": ok,
                    "status_code": status_code,
                    "error": error_message,
                    "request_payload": payload,
                    "response_json": response_json,
                    "response_text": None if response_json is not None else response_text,
                }
            )
        except error.HTTPError as exc:
            response_text = exc.read().decode("utf-8", errors="replace")
            try:
                response_json = json.loads(response_text)
            except Exception:
                response_json = None

            results.append(
                {
                    "index": index,
                    "method": method,
                    "title_method": case["title_method"],
                    "ok": False,
                    "status_code": exc.code,
                    "error": extract_error_message(exc.code, response_text, response_json),
                    "request_payload": payload,
                    "response_json": response_json,
                    "response_text": None if response_json is not None else response_text,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "index": index,
                    "method": method,
                    "title_method": case["title_method"],
                    "ok": False,
                    "status_code": None,
                    "error": str(exc),
                    "request_payload": payload,
                    "response_json": None,
                    "response_text": None,
                }
            )

    failed = [item for item in results if not item["ok"]]
    return {
        "started_at": started_at,
        "finished_at": datetime.now().isoformat(),
        "url": url,
        "total": len(results),
        "success_count": len(results) - len(failed),
        "failed_count": len(failed),
        "results": results,
        "failed_results": failed,
    }


def write_reports(report: Dict[str, Any], output_dir: Path) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    full_report_path = output_dir / f"amazingdata_call_report_{timestamp}.json"
    failed_report_path = output_dir / f"amazingdata_call_errors_{timestamp}.json"
    failed_text_path = output_dir / f"amazingdata_call_errors_{timestamp}.md"

    full_report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    failed_report = {
        "started_at": report["started_at"],
        "finished_at": report["finished_at"],
        "url": report["url"],
        "total": report["total"],
        "failed_count": report["failed_count"],
        "failed_results": report["failed_results"],
    }
    failed_report_path.write_text(
        json.dumps(failed_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    lines = [
        "# AmazingData Call 接口报错汇总",
        "",
        f"- URL: `{report['url']}`",
        f"- 总数: `{report['total']}`",
        f"- 成功: `{report['success_count']}`",
        f"- 失败: `{report['failed_count']}`",
        "",
    ]

    if not report["failed_results"]:
        lines.extend(["全部接口调用成功。", ""])
    else:
        for item in report["failed_results"]:
            lines.extend(
                [
                    f"## {item['method']}",
                    "",
                    f"- 标题方法名: `{item['title_method']}`",
                    f"- 状态码: `{item['status_code']}`",
                    f"- 错误信息: `{item['error']}`",
                    "",
                    "### 请求体",
                    "",
                    "```json",
                    json.dumps(item["request_payload"], ensure_ascii=False, indent=2),
                    "```",
                    "",
                ]
            )

            if item["response_json"] is not None:
                lines.extend(
                    [
                        "### 响应 JSON",
                        "",
                        "```json",
                        json.dumps(item["response_json"], ensure_ascii=False, indent=2),
                        "```",
                        "",
                    ]
                )
            elif item["response_text"]:
                lines.extend(
                    [
                        "### 响应文本",
                        "",
                        "```text",
                        item["response_text"],
                        "```",
                        "",
                    ]
                )

    failed_text_path.write_text("\n".join(lines), encoding="utf-8")

    return {
        "full_report": full_report_path,
        "failed_report": failed_report_path,
        "failed_markdown": failed_text_path,
    }


def main() -> int:
    args = parse_args()

    doc_path = Path(args.doc).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    failed_json_path = Path(args.failed_json).expanduser().resolve() if args.failed_json else None

    if failed_json_path:
        if not failed_json_path.exists():
            raise FileNotFoundError(f"失败报告不存在: {failed_json_path}")
        cases = load_failed_cases(failed_json_path)
        if not cases:
            raise RuntimeError(f"未从失败报告中解析到需要重跑的接口: {failed_json_path}")
    else:
        if not doc_path.exists():
            raise FileNotFoundError(f"样例文档不存在: {doc_path}")

        markdown_text = doc_path.read_text(encoding="utf-8")
        cases = extract_cases(markdown_text)
        if not cases:
            raise RuntimeError(f"未从文档中解析到接口样例: {doc_path}")

    report = run_cases(cases=cases, url=args.url, timeout=args.timeout)
    paths = write_reports(report, output_dir=output_dir)

    print(f"接口样例总数: {report['total']}")
    print(f"成功数量: {report['success_count']}")
    print(f"失败数量: {report['failed_count']}")
    print(f"完整报告: {paths['full_report']}")
    print(f"失败 JSON: {paths['failed_report']}")
    print(f"失败 Markdown: {paths['failed_markdown']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
