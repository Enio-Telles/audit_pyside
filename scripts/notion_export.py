#!/usr/bin/env python3
"""
Simple Notion page exporter (Markdown) using an internal integration token from .env

Usage:
  python scripts/notion_export.py --page PAGE_URL_OR_ID --out output.md

This script reads `NOTION_KEY` from the repo `.env` when present.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional


def load_env(path: str = ".env") -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not os.path.exists(path):
        return env
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def extract_page_id(s: str) -> Optional[str]:
    # find a 32-hex substring
    m = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if not m:
        # maybe it's already hyphenated
        m2 = re.search(r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})", s)
        if m2:
            return m2.group(1)
        return None
    raw = m.group(1)
    # format into UUID with hyphens
    return f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"


def http_get(url: str, headers: dict) -> dict:
    try:
        import requests

        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        # fallback to urllib
        from urllib import request, error

        req = request.Request(url, headers=headers)
        try:
            with request.urlopen(req, timeout=30) as r:
                data = r.read()
                return json.loads(data.decode())
        except error.HTTPError as e:
            print(f"HTTP error: {e.code} {e.reason}")
            raise


def paginate_children(block_id: str, token: str) -> List[Dict[str, Any]]:
    all_blocks: List[Dict[str, Any]] = []
    cursor = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    while True:
        url = f"https://api.notion.com/v1/blocks/{block_id}/children?page_size=100"
        if cursor:
            url = url + f"&start_cursor={cursor}"
        data = http_get(url, headers)
        results = data.get("results", [])
        all_blocks.extend(results)
        cursor = data.get("next_cursor")
        if not cursor:
            break
    return all_blocks


def plain_text_from_rich_text(rt_list: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for r in rt_list:
        # rich text items can have plain_text or nested text.content
        if isinstance(r, dict):
            if r.get("plain_text"):
                parts.append(r.get("plain_text", ""))
            else:
                # try common nested structures
                txt = None
                if r.get("text") and isinstance(r.get("text"), dict):
                    txt = r["text"].get("content")
                if not txt:
                    txt = r.get("text", "")
                if txt:
                    parts.append(txt)
                else:
                    parts.append("")
        else:
            parts.append(str(r))
    return "".join(parts)


def render_blocks(blocks: List[Dict[str, Any]], token: str, indent: int = 0) -> str:
    out_lines: List[str] = []
    for b in blocks:
        t = b.get("type")
        content = b.get(t, {})
        prefix = " " * indent
        # helper to extract text from different content keys
        def extract_text_from_content(c: Dict[str, Any]) -> str:
            # common keys: rich_text, text, title, caption
            for key in ("rich_text", "text", "title", "caption"):
                val = c.get(key)
                if isinstance(val, list) and val:
                    return plain_text_from_rich_text(val)
                if isinstance(val, str) and val:
                    return val
            # properties (page metadata)
            props = c.get("properties") or c.get("properties", {})
            if isinstance(props, dict) and props:
                parts = []
                for k, v in props.items():
                    if isinstance(v, dict):
                        if v.get("title") and isinstance(v.get("title"), list):
                            parts.append(plain_text_from_rich_text(v.get("title")))
                        elif v.get("rich_text") and isinstance(v.get("rich_text"), list):
                            parts.append(plain_text_from_rich_text(v.get("rich_text")))
                        else:
                            # try to stringify
                            try:
                                parts.append(str(v))
                            except Exception:
                                pass
                return " | ".join([p for p in parts if p])
            return ""

        if t == "paragraph":
            out_lines.append(prefix + extract_text_from_content(content) + "\n")
        elif t in ("heading_1", "heading_2", "heading_3"):
            level = {"heading_1": "#", "heading_2": "##", "heading_3": "###"}[t]
            out_lines.append(f"{level} {extract_text_from_content(content)}\n")
        elif t in ("bulleted_list_item", "numbered_list_item"):
            marker = "- " if t == "bulleted_list_item" else "1. "
            out_lines.append(prefix + marker + extract_text_from_content(content))
            out_lines.append("\n")
            if b.get("has_children"):
                child_blocks = paginate_children(b.get("id"), token)
                out_lines.append(render_blocks(child_blocks, token, indent=indent + 2))
        elif t == "to_do":
            checked = content.get("checked", False)
            mark = "[x]" if checked else "[ ]"
            out_lines.append(prefix + f"- {mark} " + extract_text_from_content(content))
            out_lines.append("\n")
            if b.get("has_children"):
                child_blocks = paginate_children(b.get("id"), token)
                out_lines.append(render_blocks(child_blocks, token, indent=indent + 2))
        elif t == "quote":
            out_lines.append(prefix + "> " + extract_text_from_content(content) + "\n")
        elif t == "code":
            lang = content.get("language", "")
            # code blocks may have 'rich_text' or 'text'
            code_text = extract_text_from_content(content)
            out_lines.append("```" + lang)
            out_lines.append(code_text)
            out_lines.append("```\n")
        elif t == "image":
            # handle external or file
            url = None
            if content.get("type") == "external":
                url = content.get("external", {}).get("url")
            elif content.get("type") == "file":
                url = content.get("file", {}).get("url")
            else:
                # some blocks include caption or image object
                for key in ("external", "file"):
                    if content.get(key) and isinstance(content.get(key), dict):
                        url = content.get(key).get("url")
                        break
            if not url:
                # try nested structure
                for k, v in content.items():
                    if isinstance(v, dict) and v.get("url"):
                        url = v.get("url")
                        break
            if url:
                out_lines.append(prefix + f"![image]({url})\n")
        elif t == "child_page":
            out_lines.append(prefix + f"## {content.get('title', '')}\n")
        else:
            # fallback: try to pull plain_text if available
            text = plain_text_from_rich_text(content.get("text", [])) if isinstance(content.get("text", []), list) else ""
            if text:
                out_lines.append(prefix + text + "\n")
        # if block has generic children (some types) and not handled above
        if b.get("has_children") and t not in ("bulleted_list_item", "numbered_list_item", "to_do"):
            child_blocks = paginate_children(b.get("id"), token)
            out_lines.append(render_blocks(child_blocks, token, indent=indent + 2))

    return "\n".join(out_lines)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--page", required=True, help="Page URL or ID")
    p.add_argument("--out", default=None, help="Output markdown file")
    args = p.parse_args()

    env = load_env()
    token = env.get("NOTION_KEY")
    if not token:
        print("Missing NOTION_KEY in .env")
        sys.exit(2)

    page_id = extract_page_id(args.page)
    if not page_id:
        print("Unable to extract page id from input")
        sys.exit(3)

    # fetch top-level blocks for the page
    try:
        blocks = paginate_children(page_id, token)
    except Exception as e:
        print(f"Failed to fetch blocks: {e}")
        sys.exit(4)

    md = render_blocks(blocks, token)

    out_path = args.out if args.out else f"notion_export_{page_id}.md"
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(md)

    print(f"Wrote Markdown export to {out_path}")


if __name__ == "__main__":
    main()
