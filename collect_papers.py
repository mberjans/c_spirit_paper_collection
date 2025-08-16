#!/usr/bin/env python3
"""
collect_papers.py

Scan one or more "paper list" directories and collect information about their
subfolders (each assumed to correspond to a paper or topic folder).

By default, scans the Dropbox path provided by the user. Outputs a summary in
CSV or JSON format, either to stdout or to files.

Metadata extraction heuristics (best-effort, standard library only):
- Counts files, PDFs, and common metadata files (.bib, .ris, .nbib, .json, .txt/.md)
- Aggregates total size and timestamps (folder ctime/mtime, earliest/latest file mtime)
- Attempts to extract DOI, title, authors, year, and venue from:
  * .bib (regex for fields)
  * .ris / .nbib (RIS-like tags)
  * .json (search for common keys)
  * .txt / .md (regex DOI scan)
  * filenames (regex DOI scan)

Additionally, the script maintains an internal in-memory registry mapping each
scanned folder to discovered paper URLs (e.g., DOI URLs, BibTeX/RIS/JSON URLs)
and a basic download status flag inferred from the presence of PDFs.

This script intentionally avoids external dependencies to keep setup simple.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Set


DEFAULT_PAPER_DIR = (
    "/Users/Mark/Dropbox/Macbook_Pro/C-SPIRIT-Global_Center_International_Research_Center_for_Enhancing_Plant_Resilience/Relevant_papers"
)

# Default output directory is the directory containing this script
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_FILES = {
    "csv": str(SCRIPT_DIR / "papers_summary.csv"),
    "json": str(SCRIPT_DIR / "papers_summary.json"),
    "doc_registry": str(SCRIPT_DIR / "doc_registry.json"),
    "url_dict": str(SCRIPT_DIR / "url_dict.json"),
    "doi_dict": str(SCRIPT_DIR / "doi_dict.json"),
    "pubmed_id_dict": str(SCRIPT_DIR / "pubmed_id_dict.json"),
    "pmc_id_dict": str(SCRIPT_DIR / "pmc_id_dict.json"),
}


DOI_REGEX = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE)

# Simple URL extractor for plain text
URL_REGEX = re.compile(r"https?://[^\s)]+", re.IGNORECASE)

# Module-level registry: folder_path -> metadata about discovered URLs and download status
PAPER_URL_REGISTRY: Dict[str, Dict[str, Any]] = {}

# Module-level registry: absolute file path -> parsing status/details
DOCUMENT_PARSE_REGISTRY: Dict[str, Dict[str, Any]] = {}

# Link-type specific dictionaries (deduplicated):
# Map identifier -> set of absolute file paths where found
URL_DICT: Dict[str, Set[str]] = {}
DOI_DICT: Dict[str, Set[str]] = {}
PUBMED_ID_DICT: Dict[str, Set[str]] = {}
PMC_ID_DICT: Dict[str, Set[str]] = {}

# Per-DOI bibliographic records aggregated from any source file
# DOI -> { title, authors, year, venue, sources: set[str] }
DOI_IN_TEXT_DICT: Dict[str, Dict[str, Any]] = {}


@dataclass
class ParseControl:
    remaining_documents_to_parse: int
    skip_already_parsed: bool


def _check_parse_gate(file_path: Path, parse_control: Optional["ParseControl"]) -> Tuple[bool, Optional[str]]:
    if parse_control is None:
        return True, None
    key = str(file_path.resolve())
    if parse_control.skip_already_parsed:
        existing = DOCUMENT_PARSE_REGISTRY.get(key)
        if existing and existing.get("parsed") is True:
            return False, "skipped_already_parsed"
    if parse_control.remaining_documents_to_parse <= 0:
        return False, "skipped_by_limit"
    return True, None


def _register_document_status(
    file_path: Path,
    folder_path: Path,
    kind: str,
    parsed: bool,
    error: Optional[str] = None,
    info: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        stat = file_path.stat()
        size = stat.st_size
        mtime = stat.st_mtime
    except Exception:
        size = None
        mtime = None
    DOCUMENT_PARSE_REGISTRY[str(file_path.resolve())] = {
        "folder_path": str(folder_path.resolve()),
        "kind": kind,
        "parsed": parsed,
        "error": error or "",
        "size_bytes": size,
        "mtime_iso": format_iso(mtime),
        "info": info or {},
    }


@dataclass
class FolderSummary:
    root_input_dir: str
    folder_name: str
    folder_path: str
    is_symlink: bool
    num_files_total: int
    num_dirs_total: int
    num_pdfs: int
    num_bibtex: int
    num_ris: int
    num_nbib: int
    num_json: int
    num_txt_md: int
    total_size_bytes: int
    folder_ctime_iso: str
    folder_mtime_iso: str
    earliest_file_mtime_iso: str
    latest_file_mtime_iso: str
    example_pdf: str
    doi: str
    title: str
    authors: str
    year: str
    venue: str


def format_iso(ts: Optional[float]) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(ts).isoformat(timespec="seconds")
    except Exception:
        return ""


def iter_subdirs(
    root: Path,
    max_depth: int = 1,
    include_hidden: bool = False,
    follow_symlinks: bool = False,
) -> Iterable[Tuple[Path, int]]:
    """Yield (directory_path, depth) for subdirectories up to max_depth.

    depth=0 corresponds to immediate children of root.
    """
    try:
        with os.scandir(root) as it:
            for entry in it:
                if not entry.is_dir(follow_symlinks=follow_symlinks):
                    continue
                name = entry.name
                if not include_hidden and name.startswith('.'):
                    continue
                subdir = Path(entry.path)
                yield subdir, 0
                # Descend further if requested
                if max_depth > 1:
                    yield from _iter_subdirs_recursive(
                        subdir, current_depth=1, max_depth=max_depth,
                        include_hidden=include_hidden, follow_symlinks=follow_symlinks
                    )
    except FileNotFoundError:
        return


def _iter_subdirs_recursive(
    current_dir: Path,
    current_depth: int,
    max_depth: int,
    include_hidden: bool,
    follow_symlinks: bool,
) -> Iterable[Tuple[Path, int]]:
    if current_depth >= max_depth:
        return
    try:
        with os.scandir(current_dir) as it:
            for entry in it:
                if not entry.is_dir(follow_symlinks=follow_symlinks):
                    continue
                name = entry.name
                if not include_hidden and name.startswith('.'):
                    continue
                subdir = Path(entry.path)
                yield subdir, current_depth
                yield from _iter_subdirs_recursive(
                    subdir,
                    current_depth=current_depth + 1,
                    max_depth=max_depth,
                    include_hidden=include_hidden,
                    follow_symlinks=follow_symlinks,
                )
    except PermissionError:
        return


def safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def find_doi_in_text(text: str) -> Optional[str]:
    match = DOI_REGEX.search(text)
    if match:
        return match.group(0)
    return None


def find_urls_in_text(text: str) -> List[str]:
    return list({m.group(0).strip() for m in URL_REGEX.finditer(text)})


def doi_to_url(doi: str) -> str:
    # Strip any leading 'doi:'
    clean = re.sub(r"^doi:\s*", "", doi, flags=re.IGNORECASE).strip()
    return f"https://doi.org/{clean}"


# PubMed / PMC patterns
PMID_NUM_REGEX = re.compile(r"\bPMID\s*:?\s*(\d{4,9})\b", re.IGNORECASE)
PMCID_REGEX = re.compile(r"\bPMCID?\s*:?\s*(PMC\d+)\b", re.IGNORECASE)
PUBMED_URL_REGEX = re.compile(r"pubmed\.ncbi\.nlm\.nih\.gov/(\d{4,9})", re.IGNORECASE)
PMC_URL_REGEX = re.compile(r"ncbi\.nlm\.nih\.gov/pmc/articles/(PMC\d+)", re.IGNORECASE)


def _add_to_dict_set(d: Dict[str, Set[str]], key: str, source: Path) -> None:
    if not key:
        return
    abs_src = str(source.resolve())
    if key not in d:
        d[key] = {abs_src}
    else:
        d[key].add(abs_src)


def update_doi_record(doi: Optional[str], source: Path, *, title: Optional[str] = None, authors: Optional[str] = None, year: Optional[str] = None, venue: Optional[str] = None) -> None:
    if not doi:
        return
    key = doi.strip()
    if not key:
        return
    rec = DOI_IN_TEXT_DICT.get(key)
    if rec is None:
        rec = {"title": None, "authors": None, "year": None, "venue": None, "sources": set()}
        DOI_IN_TEXT_DICT[key] = rec
    # Only fill missing fields to keep the first-found canonical record
    if title and not rec.get("title"):
        rec["title"] = title
    if authors and not rec.get("authors"):
        rec["authors"] = authors
    if year and not rec.get("year"):
        rec["year"] = year
    if venue and not rec.get("venue"):
        rec["venue"] = venue
    # Track source file
    rec["sources"].add(str(source.resolve()))


def extract_identifiers_from_text(text: str) -> Tuple[List[str], List[str], List[str], List[str]]:
    """Return (urls, dois, pmids, pmc_ids) found in the text.

    Duplicates are removed while preserving first-seen order.
    """
    urls_seen: Set[str] = set()
    dois_seen: Set[str] = set()
    pmids_seen: Set[str] = set()
    pmc_seen: Set[str] = set()
    urls: List[str] = []
    dois: List[str] = []
    pmids: List[str] = []
    pmcs: List[str] = []

    # URLs
    for m in URL_REGEX.finditer(text):
        u = m.group(0).strip().rstrip(').,;]')
        if u and u not in urls_seen:
            urls_seen.add(u)
            urls.append(u)
        # Derive IDs from known URL patterns
        pm_m = PUBMED_URL_REGEX.search(u)
        if pm_m:
            pid = pm_m.group(1)
            if pid not in pmids_seen:
                pmids_seen.add(pid)
                pmids.append(pid)
        pmc_m = PMC_URL_REGEX.search(u)
        if pmc_m:
            pc = pmc_m.group(1)
            if pc not in pmc_seen:
                pmc_seen.add(pc)
                pmcs.append(pc)
        # DOI in URL (doi.org)
        if "doi.org/" in u.lower():
            dm = re.search(r"doi\.org/([^\s?#]+)", u, flags=re.IGNORECASE)
            if dm:
                dstr = dm.group(1)
                # Reconstruct canonical form
                if dstr and dstr not in dois_seen:
                    dois_seen.add(dstr)
                    dois.append(dstr)

    # DOI bare
    for m in DOI_REGEX.finditer(text):
        dstr = m.group(0)
        if dstr not in dois_seen:
            dois_seen.add(dstr)
            dois.append(dstr)

    # PMID mentions
    for m in PMID_NUM_REGEX.finditer(text):
        pid = m.group(1)
        if pid not in pmids_seen:
            pmids_seen.add(pid)
            pmids.append(pid)

    # PMCID mentions
    for m in PMCID_REGEX.finditer(text):
        pc = m.group(1)
        if pc not in pmc_seen:
            pmc_seen.add(pc)
            pmcs.append(pc)

    return urls, dois, pmids, pmcs


def update_link_dicts_from_text(text: str, source: Path) -> None:
    urls, dois, pmids, pmcs = extract_identifiers_from_text(text)
    for u in urls:
        _add_to_dict_set(URL_DICT, u, source)
    for d in dois:
        _add_to_dict_set(DOI_DICT, d, source)
    for p in pmids:
        _add_to_dict_set(PUBMED_ID_DICT, p, source)
    for c in pmcs:
        _add_to_dict_set(PMC_ID_DICT, c, source)

def parse_bibtex_for_metadata(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], List[str]]:
    """Return (doi, title, authors, year, urls) from bibtex text.

    This is a very lightweight regex-based parser and may not cover all edge cases.
    """
    # DOI
    doi = None
    # Typical bibtex: doi = {10.1111/abcd.12345}
    doi_match = re.search(r"\bdoi\s*=\s*[\"{]([^\"}]+)[\"}]", text, re.IGNORECASE)
    if doi_match:
        doi = doi_match.group(1).strip()
    else:
        doi = find_doi_in_text(text)

    # Title
    title = None
    title_match = re.search(r"\btitle\s*=\s*[\"{]([^\n\r}]+)[\"}]", text, re.IGNORECASE)
    if title_match:
        title = title_match.group(1).strip()

    # Authors (raw string)
    authors = None
    authors_match = re.search(r"\bauthor\s*=\s*[\"{]([^\n\r}]+)[\"}]", text, re.IGNORECASE)
    if authors_match:
        authors = authors_match.group(1).strip()

    # Year
    year = None
    year_match = re.search(r"\byear\s*=\s*[\"{]?([0-9]{4})", text, re.IGNORECASE)
    if year_match:
        year = year_match.group(1)

    # URLs (one or more url fields)
    urls: List[str] = []
    for m in re.finditer(r"\burl\s*=\s*[\"{]([^\"}]+)[\"}]", text, re.IGNORECASE):
        urls.append(m.group(1).strip())

    return doi, title, authors, year, urls


def parse_ris_like_for_metadata(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], List[str]]:
    """Parse RIS / NBIB-like content. Return (doi, title, authors, year, venue, urls)."""
    doi = None
    title = None
    authors_list: List[str] = []
    year = None
    venue = None
    urls: List[str] = []

    for line in text.splitlines():
        # Format: XX  - value
        if ' - ' not in line:
            continue
        tag, value = line.split(' - ', 1)
        tag = tag.strip().upper()
        value = value.strip()
        if not value:
            continue
        if tag in {"DO", "DOI"} and not doi:
            doi = value
        elif tag in {"TI", "T1"} and not title:
            title = value
        elif tag in {"AU", "A1", "A2"}:
            authors_list.append(value)
        elif tag in {"PY", "Y1", "DA"} and not year:
            # Extract leading 4-digit year if present
            m = re.search(r"(19|20)\d{2}", value)
            if m:
                year = m.group(0)
        elif tag in {"JO", "JF", "T2", "BT", "J2"} and not venue:
            venue = value
        elif tag in {"UR", "L1", "L2", "LK"}:
            urls.append(value)

    authors = "; ".join(authors_list) if authors_list else None
    return doi, title, authors, year, venue, urls


def parse_json_for_metadata(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], List[str]]:
    """Search common keys in JSON for metadata. Return (doi, title, authors, year, venue, urls)."""
    try:
        data = json.loads(text)
    except Exception:
        return None, None, None, None, None, []

    def get_ci(d: Dict, *keys: str) -> Optional[str]:
        for k in keys:
            for key in d.keys():
                if key.lower() == k.lower():
                    v = d[key]
                    if isinstance(v, (str, int)):
                        return str(v)
                    if isinstance(v, list):
                        return "; ".join(map(str, v))
        return None

    doi = get_ci(data, "doi") or find_doi_in_text(text)
    title = get_ci(data, "title", "paper_title")
    authors_val = data.get("authors") or data.get("author")
    if isinstance(authors_val, list):
        authors = "; ".join(map(str, authors_val))
    elif isinstance(authors_val, str):
        authors = authors_val
    else:
        authors = None
    year = get_ci(data, "year", "publicationYear")
    venue = get_ci(data, "journal", "venue", "journalName")

    urls: List[str] = []
    # Common URL keys
    for k in ["url", "pdf_url", "link"]:
        v = data.get(k)
        if isinstance(v, str):
            urls.append(v)
        elif isinstance(v, list):
            urls.extend([str(x) for x in v])
    # links may be list[dict]
    links = data.get("links")
    if isinstance(links, list):
        for item in links:
            if isinstance(item, dict):
                u = item.get("url") or item.get("href")
                if isinstance(u, str):
                    urls.append(u)

    return doi, title, authors, year, venue, urls


def extract_metadata_from_folder(folder: Path, parse_control: Optional[ParseControl] = None) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], List[str]]:
    """Aggregate best-effort metadata from files inside the folder (recursive)."""
    best_doi = None
    best_title = None
    best_authors = None
    best_year = None
    best_venue = None
    collected_urls: List[str] = []

    for dirpath, _, filenames in os.walk(folder):
        for fname in filenames:
            path = Path(dirpath) / fname
            lower = fname.lower()

            # Try to find DOI in filename as a quick win
            if best_doi is None:
                doi_in_name = find_doi_in_text(fname)
                if doi_in_name:
                    best_doi = doi_in_name

            if lower.endswith('.bib'):
                text = safe_read_text(path)
                try:
                    allowed, reason = _check_parse_gate(path, parse_control)
                    if not allowed:
                        _register_document_status(path, folder, kind="bib", parsed=False, error=reason)
                        continue
                    doi, title, authors, year, urls = parse_bibtex_for_metadata(text)
                    # Try to get a journal/booktitle field for venue
                    m = re.search(r"\b(journal|booktitle)\s*=\s*[\"{]([^\n\r}]+)[\"}]", text, re.IGNORECASE)
                    venue_guess = m.group(2).strip() if m else None
                    # Update link dictionaries from raw text and bib URLs
                    all_urls, all_dois, all_pmids, all_pmcs = extract_identifiers_from_text(text)
                    update_link_dicts_from_text(text, path)
                    for u in urls:
                        _add_to_dict_set(URL_DICT, u, path)
                    # Update DOI record with available metadata
                    update_doi_record(doi, path, title=title, authors=authors, year=year, venue=venue_guess)
                    for d_ in all_dois:
                        if d_ != doi:
                            update_doi_record(d_, path)
                    _register_document_status(
                        path, folder, kind="bib", parsed=True, error=None,
                        info={
                            "found_doi": bool(doi),
                            "found_title": bool(title),
                            "found_authors": bool(authors),
                            "found_year": bool(year),
                            "urls_found": len(urls),
                            "urls_in_text": len(all_urls),
                            "dois_in_text": len(all_dois),
                            "pmids_in_text": len(all_pmids),
                            "pmcids_in_text": len(all_pmcs),
                        },
                    )
                except Exception as exc:
                    _register_document_status(path, folder, kind="bib", parsed=False, error=str(exc))
                    doi, title, authors, year, urls = None, None, None, None, []
                else:
                    if parse_control is not None:
                        parse_control.remaining_documents_to_parse -= 1
                best_doi = best_doi or doi
                best_title = best_title or title
                best_authors = best_authors or authors
                best_year = best_year or year
                collected_urls.extend(urls)
            elif lower.endswith('.ris') or lower.endswith('.nbib'):
                text = safe_read_text(path)
                try:
                    allowed, reason = _check_parse_gate(path, parse_control)
                    if not allowed:
                        _register_document_status(path, folder, kind="ris_nbib", parsed=False, error=reason)
                        continue
                    doi, title, authors, year, venue, urls = parse_ris_like_for_metadata(text)
                    all_urls, all_dois, all_pmids, all_pmcs = extract_identifiers_from_text(text)
                    update_link_dicts_from_text(text, path)
                    for u in urls:
                        _add_to_dict_set(URL_DICT, u, path)
                    # Update DOI record
                    update_doi_record(doi, path, title=title, authors=authors, year=year, venue=venue)
                    for d_ in all_dois:
                        if d_ != doi:
                            update_doi_record(d_, path)
                    _register_document_status(
                        path, folder, kind="ris" if lower.endswith('.ris') else "nbib", parsed=True, error=None,
                        info={
                            "found_doi": bool(doi),
                            "found_title": bool(title),
                            "found_authors": bool(authors),
                            "found_year": bool(year),
                            "found_venue": bool(venue),
                            "urls_found": len(urls),
                            "urls_in_text": len(all_urls),
                            "dois_in_text": len(all_dois),
                            "pmids_in_text": len(all_pmids),
                            "pmcids_in_text": len(all_pmcs),
                        },
                    )
                except Exception as exc:
                    _register_document_status(path, folder, kind="ris_nbib", parsed=False, error=str(exc))
                    doi, title, authors, year, venue, urls = None, None, None, None, None, []
                else:
                    if parse_control is not None:
                        parse_control.remaining_documents_to_parse -= 1
                best_doi = best_doi or doi
                best_title = best_title or title
                best_authors = best_authors or authors
                best_year = best_year or year
                best_venue = best_venue or venue
                collected_urls.extend(urls)
            elif lower.endswith('.json'):
                text = safe_read_text(path)
                try:
                    allowed, reason = _check_parse_gate(path, parse_control)
                    if not allowed:
                        _register_document_status(path, folder, kind="json", parsed=False, error=reason)
                        continue
                    doi, title, authors, year, venue, urls = parse_json_for_metadata(text)
                    all_urls, all_dois, all_pmids, all_pmcs = extract_identifiers_from_text(text)
                    update_link_dicts_from_text(text, path)
                    for u in urls:
                        _add_to_dict_set(URL_DICT, u, path)
                    # Update DOI record
                    update_doi_record(doi, path, title=title, authors=authors, year=year, venue=venue)
                    for d_ in all_dois:
                        if d_ != doi:
                            update_doi_record(d_, path)
                    _register_document_status(
                        path, folder, kind="json", parsed=True, error=None,
                        info={
                            "found_doi": bool(doi),
                            "found_title": bool(title),
                            "found_authors": bool(authors),
                            "found_year": bool(year),
                            "found_venue": bool(venue),
                            "urls_found": len(urls),
                            "urls_in_text": len(all_urls),
                            "dois_in_text": len(all_dois),
                            "pmids_in_text": len(all_pmids),
                            "pmcids_in_text": len(all_pmcs),
                        },
                    )
                except Exception as exc:
                    _register_document_status(path, folder, kind="json", parsed=False, error=str(exc))
                    doi, title, authors, year, venue, urls = None, None, None, None, None, []
                else:
                    if parse_control is not None:
                        parse_control.remaining_documents_to_parse -= 1
                best_doi = best_doi or doi
                best_title = best_title or title
                best_authors = best_authors or authors
                best_year = best_year or year
                best_venue = best_venue or venue
                collected_urls.extend(urls)
            elif lower.endswith('.txt') or lower.endswith('.md'):
                text = safe_read_text(path)
                try:
                    allowed, reason = _check_parse_gate(path, parse_control)
                    if not allowed:
                        _register_document_status(path, folder, kind="txt_md", parsed=False, error=reason)
                        continue
                    doi = find_doi_in_text(text)
                    if best_doi is None and doi:
                        best_doi = doi
                    urls_found = find_urls_in_text(text)
                    collected_urls.extend(urls_found)
                    all_urls, all_dois, all_pmids, all_pmcs = extract_identifiers_from_text(text)
                    update_link_dicts_from_text(text, path)
                    # Update DOI records for all found DOIs
                    for d_ in all_dois:
                        update_doi_record(d_, path)
                    _register_document_status(
                        path, folder, kind="txt_md", parsed=True, error=None,
                        info={
                            "found_doi": bool(doi),
                            "urls_found": len(urls_found),
                            "urls_in_text": len(all_urls),
                            "dois_in_text": len(all_dois),
                            "pmids_in_text": len(all_pmids),
                            "pmcids_in_text": len(all_pmcs),
                        },
                    )
                except Exception as exc:
                    _register_document_status(path, folder, kind="txt_md", parsed=False, error=str(exc))
                else:
                    if parse_control is not None:
                        parse_control.remaining_documents_to_parse -= 1
            elif lower.endswith('.docx'):
                # Best-effort: extract text from docx (zip with XML content)
                try:
                    allowed, reason = _check_parse_gate(path, parse_control)
                    if not allowed:
                        _register_document_status(path, folder, kind="docx", parsed=False, error=reason)
                        continue
                    import zipfile
                    text_parts: List[str] = []
                    with zipfile.ZipFile(path) as zf:
                        for name in [
                            'word/document.xml',
                            'word/footnotes.xml',
                            'word/endnotes.xml',
                            'word/header1.xml',
                            'word/footer1.xml',
                        ]:
                            if name in zf.namelist():
                                data = zf.read(name)
                                try:
                                    from xml.etree import ElementTree as ET
                                    root = ET.fromstring(data)
                                    text_parts.append(''.join(root.itertext()))
                                except Exception:
                                    # Fallback: strip tags
                                    s = data.decode('utf-8', errors='ignore')
                                    s = re.sub(r'<[^>]+>', ' ', s)
                                    text_parts.append(s)
                    combined = '\n'.join(text_parts)
                    all_urls, all_dois, all_pmids, all_pmcs = extract_identifiers_from_text(combined)
                    update_link_dicts_from_text(combined, path)
                    for d_ in all_dois:
                        update_doi_record(d_, path)
                    _register_document_status(
                        path, folder, kind="docx", parsed=True, error=None,
                        info={
                            "urls_in_text": len(all_urls),
                            "dois_in_text": len(all_dois),
                            "pmids_in_text": len(all_pmids),
                            "pmcids_in_text": len(all_pmcs),
                        },
                    )
                except Exception as exc:
                    _register_document_status(path, folder, kind="docx", parsed=False, error=str(exc))
                else:
                    if parse_control is not None:
                        parse_control.remaining_documents_to_parse -= 1
            elif lower.endswith('.pdf'):
                # Attempt to pull text-like content from PDF for link/ID detection
                try:
                    allowed, reason = _check_parse_gate(path, parse_control)
                    if not allowed:
                        _register_document_status(path, folder, kind="pdf", parsed=False, error=reason, info={"is_pdf": True})
                        continue
                    raw = path.read_bytes()
                    # Try multiple decodings
                    try:
                        txt = raw.decode('utf-8', errors='ignore')
                    except Exception:
                        txt = raw.decode('latin-1', errors='ignore')
                    # Also search for explicit /URI( ... ) entries
                    uri_candidates = re.findall(rb"/URI\s*\(([^)]+)\)", raw)
                    for b in uri_candidates:
                        try:
                            candidate = b.decode('utf-8', errors='ignore')
                            txt += '\n' + candidate
                        except Exception:
                            pass
                    all_urls, all_dois, all_pmids, all_pmcs = extract_identifiers_from_text(txt)
                    update_link_dicts_from_text(txt, path)
                    for d_ in all_dois:
                        update_doi_record(d_, path)
                    _register_document_status(
                        path, folder, kind="pdf", parsed=True, error=None,
                        info={
                            "is_pdf": True,
                            "urls_in_text": len(all_urls),
                            "dois_in_text": len(all_dois),
                            "pmids_in_text": len(all_pmids),
                            "pmcids_in_text": len(all_pmcs),
                        },
                    )
                except Exception as exc:
                    _register_document_status(path, folder, kind="pdf", parsed=False, error=str(exc), info={"is_pdf": True})
                else:
                    if parse_control is not None:
                        parse_control.remaining_documents_to_parse -= 1

            # Early exit if we already have strong metadata
            if best_doi and best_title and best_authors and best_year:
                break

    # Deduplicate URLs while preserving order
    seen: set = set()
    deduped_urls = []
    for u in collected_urls:
        if u and u not in seen:
            seen.add(u)
            deduped_urls.append(u)

    return best_doi, best_title, best_authors, best_year, best_venue, deduped_urls


def summarize_folder(root_input_dir: Path, folder: Path) -> Tuple[FolderSummary, List[str]]:
    num_files_total = 0
    num_dirs_total = 0
    num_pdfs = 0
    num_bibtex = 0
    num_ris = 0
    num_nbib = 0
    num_json = 0
    num_txt_md = 0
    total_size_bytes = 0
    earliest_mtime: Optional[float] = None
    latest_mtime: Optional[float] = None
    example_pdf = ""

    for dirpath, dirnames, filenames in os.walk(folder):
        # Count subdirectories (descendants)
        num_dirs_total += len(dirnames)
        for fname in filenames:
            num_files_total += 1
            path = Path(dirpath) / fname
            try:
                stat = path.stat()
                total_size_bytes += stat.st_size
                mtime = stat.st_mtime
                if earliest_mtime is None or mtime < earliest_mtime:
                    earliest_mtime = mtime
                if latest_mtime is None or mtime > latest_mtime:
                    latest_mtime = mtime
            except FileNotFoundError:
                continue

            lower = fname.lower()
            if lower.endswith('.pdf'):
                num_pdfs += 1
                if not example_pdf:
                    example_pdf = str(path)
                _register_document_status(
                    path, folder, kind="pdf", parsed=False, error=None,
                    info={"is_pdf": True},
                )
            elif lower.endswith('.bib'):
                num_bibtex += 1
            elif lower.endswith('.ris'):
                num_ris += 1
            elif lower.endswith('.nbib'):
                num_nbib += 1
            elif lower.endswith('.json'):
                num_json += 1
            elif lower.endswith('.txt') or lower.endswith('.md'):
                num_txt_md += 1

    try:
        fstat = folder.stat()
        folder_ctime = fstat.st_ctime
        folder_mtime = fstat.st_mtime
        is_symlink = folder.is_symlink()
    except FileNotFoundError:
        folder_ctime = None
        folder_mtime = None
        is_symlink = False

    # Use ParseControl constructed at call site
    doi, title, authors, year, venue, urls = extract_metadata_from_folder(folder, parse_control=_GLOBAL_PARSE_CONTROL)

    summary = FolderSummary(
        root_input_dir=str(root_input_dir),
        folder_name=folder.name,
        folder_path=str(folder.resolve()),
        is_symlink=is_symlink,
        num_files_total=num_files_total,
        num_dirs_total=num_dirs_total,
        num_pdfs=num_pdfs,
        num_bibtex=num_bibtex,
        num_ris=num_ris,
        num_nbib=num_nbib,
        num_json=num_json,
        num_txt_md=num_txt_md,
        total_size_bytes=total_size_bytes,
        folder_ctime_iso=format_iso(folder_ctime),
        folder_mtime_iso=format_iso(folder_mtime),
        earliest_file_mtime_iso=format_iso(earliest_mtime),
        latest_file_mtime_iso=format_iso(latest_mtime),
        example_pdf=example_pdf,
        doi=doi or "",
        title=title or "",
        authors=authors or "",
        year=year or "",
        venue=venue or "",
    )

    return summary, urls


def write_csv(summaries: List[FolderSummary], output: Optional[Path]) -> None:
    fieldnames = list(asdict(summaries[0]).keys()) if summaries else [
        "root_input_dir", "folder_name", "folder_path", "is_symlink",
        "num_files_total", "num_dirs_total", "num_pdfs", "num_bibtex", "num_ris",
        "num_nbib", "num_json", "num_txt_md", "total_size_bytes",
        "folder_ctime_iso", "folder_mtime_iso", "earliest_file_mtime_iso",
        "latest_file_mtime_iso", "example_pdf", "doi", "title", "authors",
        "year", "venue",
    ]

    if output:
        out_stream = output.open('w', newline='', encoding='utf-8')
        close_when_done = True
    else:
        out_stream = sys.stdout
        close_when_done = False

    try:
        writer = csv.DictWriter(out_stream, fieldnames=fieldnames)
        writer.writeheader()
        for s in summaries:
            writer.writerow(asdict(s))
    finally:
        if close_when_done:
            out_stream.close()


def write_json(summaries: List[FolderSummary], output: Optional[Path], mode: str = "append") -> None:
    data = [asdict(s) for s in summaries]
    if output is None:
        sys.stdout.write(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
        return
    # Append mode: extend existing list if possible
    if mode == "append" and output.exists():
        try:
            existing = json.loads(output.read_text(encoding='utf-8'))
            if isinstance(existing, list):
                existing.extend(data)
                output.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding='utf-8')
                return
        except Exception:
            pass
    # Overwrite or fallback
    output.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect information about subfolders in paper list directories.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-d", "--dir", "--path", "--input",
        dest="paths",
        action="append",
        default=None,
        help=(
            "Directory to scan. Repeatable. "
            "If omitted, defaults to the user's Dropbox path."
        ),
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=1,
        help="How deep to scan for subfolders (1 = only immediate subfolders).",
    )
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="Include hidden directories (names starting with a dot).",
    )
    parser.add_argument(
        "--follow-symlinks",
        action="store_true",
        help="Follow symlinked directories when scanning.",
    )
    parser.add_argument(
        "--stdout-format",
        choices=["csv", "json", "none"],
        default="none",
        help="Format to emit to stdout. Use 'none' to suppress stdout output.",
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default=None,
        help="Optional path to write CSV summary to a file.",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Optional path to write JSON summary to a file.",
    )
    parser.add_argument(
        "--output-doc-registry",
        type=str,
        default=None,
        help="Optional path to write per-document parse registry as JSON.",
    )
    parser.add_argument(
        "--output-url-dict",
        type=str,
        default=None,
        help="Optional path to write URL dictionary as JSON (URL -> list of file paths).",
    )
    parser.add_argument(
        "--output-doi-dict",
        type=str,
        default=None,
        help="Optional path to write DOI dictionary as JSON (DOI -> list of file paths).",
    )
    parser.add_argument(
        "--output-pubmed-id-dict",
        type=str,
        default=None,
        help="Optional path to write PubMed ID dictionary as JSON (PMID -> list of file paths).",
    )
    parser.add_argument(
        "--output-pmc-id-dict",
        type=str,
        default=None,
        help="Optional path to write PMC ID dictionary as JSON (PMCID -> list of file paths).",
    )
    parser.add_argument(
        "--only-with-pdfs",
        action="store_true",
        help="Only include subfolders that contain at least one PDF.",
    )
    parser.add_argument(
        "--paper-max",
        type=int,
        default=1,
        help="Maximum number of documents to parse this run (across all folders).",
    )
    parser.add_argument(
        "--skip-parsed",
        action="store_true",
        help="Skip documents already marked as parsed in the registry (default: process again).",
    )
    parser.add_argument(
        "--no-default-outputs",
        action="store_true",
        help="Do not write default output files when specific --output-* flags are not provided.",
    )
    parser.add_argument(
        "--output-write-mode",
        choices=["append", "overwrite"],
        default="append",
        help="How to write JSON outputs: append merges with existing files; overwrite replaces.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    input_dirs: List[Path] = []
    candidate_paths = args.paths if args.paths else [DEFAULT_PAPER_DIR]
    for p in candidate_paths:
        path = Path(p).expanduser()
        if not path.exists():
            print(f"Warning: input path does not exist: {path}", file=sys.stderr)
            continue
        if not path.is_dir():
            print(f"Warning: input path is not a directory: {path}", file=sys.stderr)
            continue
        input_dirs.append(path)

    if not input_dirs:
        print("No valid input directories. Exiting.", file=sys.stderr)
        return 2

    # Initialize global parse control for this run
    global _GLOBAL_PARSE_CONTROL
    _GLOBAL_PARSE_CONTROL = ParseControl(
        remaining_documents_to_parse=max(0, int(args.paper_max)),
        skip_already_parsed=bool(args.skip_parsed),
    )

    # If not disabled, set default output paths when flags are not provided
    if not args.no_default_outputs:
        if args.output_csv is None:
            args.output_csv = DEFAULT_OUTPUT_FILES["csv"]
        if args.output_json is None:
            args.output_json = DEFAULT_OUTPUT_FILES["json"]
        if args.output_doc_registry is None:
            args.output_doc_registry = DEFAULT_OUTPUT_FILES["doc_registry"]
        if getattr(args, "output_url_dict", None) is None:
            args.output_url_dict = DEFAULT_OUTPUT_FILES["url_dict"]
        if getattr(args, "output_doi_dict", None) is None:
            args.output_doi_dict = DEFAULT_OUTPUT_FILES["doi_dict"]
        if getattr(args, "output_pubmed_id_dict", None) is None:
            args.output_pubmed_id_dict = DEFAULT_OUTPUT_FILES["pubmed_id_dict"]
        if getattr(args, "output_pmc_id_dict", None) is None:
            args.output_pmc_id_dict = DEFAULT_OUTPUT_FILES["pmc_id_dict"]

    summaries: List[FolderSummary] = []
    for root_dir in input_dirs:
        for subdir, _depth in iter_subdirs(
            root_dir,
            max_depth=args.max_depth,
            include_hidden=args.include_hidden,
            follow_symlinks=args.follow_symlinks,
        ):
            # If limit reached, stop scanning further
            if _GLOBAL_PARSE_CONTROL.remaining_documents_to_parse <= 0:
                break
            try:
                summary, urls = summarize_folder(root_dir, subdir)
            except Exception as exc:
                print(f"Error summarizing {subdir}: {exc}", file=sys.stderr)
                continue
            if args.only_with_pdfs and summary.num_pdfs < 1:
                continue
            summaries.append(summary)
            # Update in-memory registry
            folder_key = summary.folder_path
            doi_url = doi_to_url(summary.doi) if summary.doi else None
            # Consider download status true if at least one PDF exists
            PAPER_URL_REGISTRY[folder_key] = {
                "folder": summary.folder_name,
                "root": summary.root_input_dir,
                "urls": [u for u in urls if isinstance(u, str)] + ([doi_url] if doi_url else []),
                "has_pdf": summary.num_pdfs > 0,
                "example_pdf": summary.example_pdf,
            }

        # If limit reached, stop outer loop as well
        if _GLOBAL_PARSE_CONTROL.remaining_documents_to_parse <= 0:
            break

    # Sort by folder name for stable output
    summaries.sort(key=lambda s: (s.root_input_dir, s.folder_name.lower()))

    # Emit to stdout
    if args.stdout_format != "none":
        if args.stdout_format == "csv":
            write_csv(summaries, output=None)
        elif args.stdout_format == "json":
            write_json(summaries, output=None)

    # Optionally write files
    if args.output_csv:
        write_csv(summaries, output=Path(args.output_csv).expanduser())
    if args.output_json:
        write_json(summaries, output=Path(args.output_json).expanduser(), mode=args.output_write_mode)

    # Optionally write the document parse registry
    if args.output_doc_registry:
        out_path = Path(args.output_doc_registry).expanduser()
        try:
            if args.output_write_mode == "append" and out_path.exists():
                try:
                    existing = json.loads(out_path.read_text(encoding="utf-8"))
                    if isinstance(existing, dict):
                        merged = dict(existing)
                        merged.update(DOCUMENT_PARSE_REGISTRY)
                        out_path.write_text(json.dumps(merged, indent=2, ensure_ascii=False), encoding="utf-8")
                    else:
                        out_path.write_text(json.dumps(DOCUMENT_PARSE_REGISTRY, indent=2, ensure_ascii=False), encoding="utf-8")
                except Exception:
                    out_path.write_text(json.dumps(DOCUMENT_PARSE_REGISTRY, indent=2, ensure_ascii=False), encoding="utf-8")
            else:
                out_path.write_text(json.dumps(DOCUMENT_PARSE_REGISTRY, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            print(f"Failed to write document registry to {out_path}: {exc}", file=sys.stderr)

    # Optionally write link-type dictionaries (convert sets to lists)
    def _dump_dict_set(d: Dict[str, Set[str]], path_str: Optional[str]) -> None:
        if not path_str:
            return
        out_path = Path(path_str).expanduser()
        try:
            serializable = {k: sorted(list(v)) for k, v in d.items()}
            if args.output_write_mode == "append" and out_path.exists():
                try:
                    existing = json.loads(out_path.read_text(encoding="utf-8"))
                    if isinstance(existing, dict):
                        # Merge lists as sets
                        for key, paths in existing.items():
                            if isinstance(paths, list):
                                serializable.setdefault(key, [])
                                # union
                                merged_set = set(serializable[key]) | set(paths)
                                serializable[key] = sorted(list(merged_set))
                    # else ignore malformed existing and overwrite with current
                except Exception:
                    pass
            out_path.write_text(json.dumps(serializable, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            print(f"Failed to write dictionary to {out_path}: {exc}", file=sys.stderr)

    _dump_dict_set(URL_DICT, args.output_url_dict)
    _dump_dict_set(DOI_DICT, args.output_doi_dict)
    _dump_dict_set(PUBMED_ID_DICT, args.output_pubmed_id_dict)
    _dump_dict_set(PMC_ID_DICT, args.output_pmc_id_dict)

    # Also write DOI_IN_TEXT_DICT alongside DOI dict if requested (same base path + .records.json)
    if args.output_doi_dict:
        try:
            base = Path(args.output_doi_dict).expanduser()
            out_path = base.parent / (base.stem + ".records.json")
            # Convert sources set to list
            serializable = {}
            for doi_key, rec in DOI_IN_TEXT_DICT.items():
                serializable[doi_key] = {
                    "title": rec.get("title"),
                    "authors": rec.get("authors"),
                    "year": rec.get("year"),
                    "venue": rec.get("venue"),
                    "sources": sorted(list(rec.get("sources", set()))),
                }
            if args.output_write_mode == "append" and out_path.exists():
                try:
                    existing = json.loads(out_path.read_text(encoding="utf-8"))
                    if isinstance(existing, dict):
                        # Merge: prefer existing non-null metadata; union sources
                        for doi_key, old in existing.items():
                            curr = serializable.get(doi_key)
                            if curr is None:
                                serializable[doi_key] = old
                            else:
                                for fld in ("title", "authors", "year", "venue"):
                                    if old.get(fld) and not curr.get(fld):
                                        curr[fld] = old.get(fld)
                                old_sources = set(old.get("sources", []) if isinstance(old.get("sources"), list) else [])
                                new_sources = set(curr.get("sources", []) if isinstance(curr.get("sources"), list) else [])
                                curr["sources"] = sorted(list(old_sources | new_sources))
                except Exception:
                    pass
            out_path.write_text(json.dumps(serializable, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            print(f"Failed to write DOI records: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


