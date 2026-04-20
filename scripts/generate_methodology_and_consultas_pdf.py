#!/usr/bin/env python3
"""
Generate `metodologia.pdf` from Markdown files in `metodologia_mds`
and `consultas.pdf` containing selected SQL files.

Saves outputs to `metodologia_mds/metodologia.pdf` and
`metodologia_mds/consultas.pdf`.

Run from repository root:
    python scripts/generate_methodology_and_consultas_pdf.py
"""
from pathlib import Path
import sys

try:
    import fitz  # PyMuPDF
except Exception as e:
    print("PyMuPDF (fitz) is required. Install with: pip install pymupdf")
    raise


ROOT = Path(__file__).resolve().parents[1]
MD_DIR = ROOT / "metodologia_mds"
SQL_DIR = ROOT / "sql"


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf8")
    except Exception:
        return path.read_text(encoding="latin-1")


def make_pdf_from_text_sections(output_path: Path, sections: list[dict]):
    doc = fitz.open()
    # A4 page size in points (approx)
    PAGE_W, PAGE_H = 595, 842
    MARGIN = 40
    header_rect = fitz.Rect(MARGIN, 20, PAGE_W - MARGIN, 60)
    content_rect = fitz.Rect(MARGIN, 70, PAGE_W - MARGIN, PAGE_H - MARGIN)

    for sec in sections:
        title = sec.get("title", "")
        content = sec.get("content", "")
        is_code = sec.get("is_code", False)
        fontname = "courier" if is_code else "helv"
        fontsize = 9 if is_code else 11
        approx_chars_per_page = 2000 if is_code else 4200

        # split into paragraphs to avoid mid-word page cuts
        paras = content.split("\n\n")
        chunk = ""
        for p in paras:
            block = p + "\n\n"
            if len(chunk) + len(block) > approx_chars_per_page and chunk:
                page = doc.new_page(-1, width=PAGE_W, height=PAGE_H)
                if title:
                    page.insert_textbox(header_rect, title, fontsize=14, fontname="helv")
                page.insert_textbox(content_rect, chunk, fontsize=fontsize, fontname=fontname)
                chunk = block
            else:
                chunk += block

        if chunk:
            page = doc.new_page(-1, width=PAGE_W, height=PAGE_H)
            if title:
                page.insert_textbox(header_rect, title, fontsize=14, fontname="helv")
            page.insert_textbox(content_rect, chunk, fontsize=fontsize, fontname=fontname)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(output_path))
    doc.close()


def main():
    # Collect markdown files (ordered)
    md_files = sorted(MD_DIR.glob("*.md"))
    sections = []
    for md in md_files:
        text = read_text(md)
        # try to extract a top-level heading as title
        title = md.stem
        for ln in text.splitlines():
            if ln.strip().startswith("#"):
                title = ln.strip().lstrip("#").strip()
                break
        sections.append({"title": title, "content": text, "is_code": False})

    out_method = MD_DIR / "metodologia.pdf"
    if sections:
        make_pdf_from_text_sections(out_method, sections)
        print("Wrote:", out_method)
    else:
        print("No markdown files found in", MD_DIR)

    # SQL files to include (explicit list)
    sql_names = ["c170.sql", "bloco_h.sql", "NFe.sql", "NFCe.sql"]
    sql_sections = []
    for name in sql_names:
        p = SQL_DIR / name
        if p.exists():
            sql_sections.append({"title": name, "content": read_text(p), "is_code": True})
        else:
            print("Missing SQL (skipped):", p)

    out_consultas = MD_DIR / "consultas.pdf"
    if sql_sections:
        make_pdf_from_text_sections(out_consultas, sql_sections)
        print("Wrote:", out_consultas)
    else:
        print("No SQL sections to write.")


if __name__ == "__main__":
    main()
