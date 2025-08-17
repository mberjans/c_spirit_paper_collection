# collect_papers.py – Paper folder scanner and metadata extractor

## Overview
`collect_papers.py` scans one or more paper list directories, summarizes their subfolders (and optionally the root folder), and extracts identifiers and basic metadata from contained documents. It produces CSV/JSON summaries and structured dictionaries for URLs, DOIs, PubMed IDs, and PMC IDs. The script is dependency-free (Python standard library only).

Key capabilities:
- Scan directory trees with configurable depth, visibility, and symlink behavior
- Summarize per-folder counts (PDFs, BibTeX, RIS/NBIB, JSON, TXT/MD) and sizes/timestamps
- Extract identifiers and metadata from multiple formats:
  - `.bib` (BibTeX): DOI, title, authors, year, venue (via `journal`/`booktitle`), url fields
  - `.ris`/`.nbib` (RIS-like): DOI, title, authors, year, venue, url fields (`UR`/`L1`/`L2`/`LK`)
  - `.json`: DOI, title, authors, year, venue, url(s)
  - `.txt`/`.md`: DOI/URL/PMID/PMCID via regex
  - `.docx`: lightweight XML text extraction and regex scan
  - `.pdf`: best-effort by scanning for text-like content and embedded `/URI(...)` entries
- Maintain in-memory registries and write them to JSON on request
- Incremental or one-off parsing with limits and cross-run skipping

## Installation
- Python 3.8+ (no external packages required)
- macOS paths with Dropbox may resolve to `~/Library/CloudStorage/Dropbox/...` (script uses absolute resolved paths)

Optional (for repo tasks): `gh` CLI for GitHub (not needed to run the script).

## Quick start
Default scan (no stdout spam; writes default files to the script directory):
```bash
python3 /Users/Mark/Research/C-Spirit/c_spirit_paper_collection/collect_papers.py --paper-max 1
```

Process a specific directory, include the root folder itself, parse two documents, and write only URL outputs:
```bash
python3 collect_papers.py \
  -d "/path/to/Relevant_papers/Undermind" \
  --include-root --paper-max 2 --no-default-outputs \
  --output-url-dict ./url_dict.json --output-write-mode overwrite --stdout-format none
```

## CLI
```text
-d, --dir, --path, --input PATH    Repeatable. Directory to scan. Defaults to the provided Dropbox path when omitted.
--include-root                     Also process each provided -d directory itself (not only its subfolders).
--max-depth INT                    Scan depth (1 = only immediate subfolders). Default: 1
--include-hidden                   Include hidden directories.
--follow-symlinks                  Follow symlinked directories.

--stdout-format csv|json|none      Emit summaries to stdout. Default: none
--output-csv PATH                  Write folder summaries (CSV)
--output-json PATH                 Write folder summaries (JSON)

--output-doc-registry PATH         Write per-document parse registry (JSON object)
--output-url-dict PATH             Write URL dictionary (JSON object: URL -> [file paths])
--output-doi-dict PATH             Write DOI dictionary (JSON object: DOI -> [file paths])
--output-pubmed-id-dict PATH       Write PubMed ID dictionary (JSON object)
--output-pmc-id-dict PATH          Write PMC ID dictionary (JSON object)
--output-write-mode append|overwrite  JSON write strategy. Default: append
--no-default-outputs               Do not write default output files when specific --output-* are not provided.

--paper-max INT                    Max number of documents to parse this run. Default: 1
--skip-parsed                      Skip documents already marked as parsed in the registry (see below)
--input-doc-registry PATH          Preload a previous document registry to enable cross-run skipping

--only-with-pdfs                   Only include subfolders that contain at least one PDF in the summaries
```

Notes:
- Default outputs (when `--no-default-outputs` is not used) are written next to the script: `papers_summary.csv`, `papers_summary.json`, `doc_registry.json`, `url_dict.json`, `doi_dict.json`, `pubmed_id_dict.json`, `pmc_id_dict.json`.
- `--output-write-mode append` merges JSON outputs:
  - Summaries (`--output-json`): list extended
  - Registries/dictionaries: key-wise merge; for DOI/URL records sidecars, existing non-null metadata is preserved and `sources` lists are unioned
- Paths are stored as absolute, resolved paths.

## What gets collected
### Folder summaries (CSV/JSON)
For each folder scanned:
- `root_input_dir`, `folder_name`, `folder_path`, `is_symlink`
- Counts: `num_files_total`, `num_dirs_total`, `num_pdfs`, `num_bibtex`, `num_ris`, `num_nbib`, `num_json`, `num_txt_md`
- Size/timestamps: `total_size_bytes`, `folder_ctime_iso`, `folder_mtime_iso`, `earliest_file_mtime_iso`, `latest_file_mtime_iso`
- Example file: `example_pdf`
- Best-effort metadata: `doi`, `title`, `authors`, `year`, `venue` (aggregated across files in folder)

### In-memory and JSON dictionaries
- `URL_DICT` (URL → set of file paths)
- `DOI_DICT` (DOI → set of file paths)
- `PUBMED_ID_DICT` (PMID → set of file paths)
- `PMC_ID_DICT` (PMCID → set of file paths)

Sidecar records with metadata:
- `DOI_IN_TEXT_DICT` (DOI → {title, authors, year, venue, sources})
- `URL_RECORDS_DICT` (URL → {doi, title, authors, year, venue, sources})

When writing `--output-doi-dict <path>`, a sidecar `<stem>.records.json` is written that serializes `DOI_IN_TEXT_DICT` (sources are lists).
When writing `--output-url-dict <path>`, a sidecar `<stem>.records.json` is written that serializes `URL_RECORDS_DICT`.

### Per-document parse registry (JSON)
`DOCUMENT_PARSE_REGISTRY` is a JSON object keyed by absolute file path. Each entry contains:
- `folder_path`, `kind` (pdf|bib|ris|nbib|json|txt_md|docx)
- `parsed` (bool), `error` (string if any), `size_bytes`, `mtime_iso`
- `info` with per-document counts:
  - `urls_in_text`, `dois_in_text`, `pmids_in_text`, `pmcids_in_text`
  - Format-specific flags (e.g., `found_doi`, `found_title`, `found_authors`, `found_year`, `found_venue`, `urls_found`)

## Scanning behavior
- By default, the script scans only immediate subfolders (`--max-depth 1`).
- Use `--include-root` to process each provided `-d` directory itself (root-level files like `references.txt`/`references.bib`).
- `--paper-max` limits how many individual documents are parsed in a run (across all scanned folders). This is useful for incremental development/testing.
- `--skip-parsed` (with `--input-doc-registry`) enables cross-run skipping of already parsed files.

## Examples
- Parse one document from default path and write default outputs:
```bash
python3 collect_papers.py --paper-max 1
```

- Parse root files in a specific folder (e.g., `Undermind`) and write only DOI dictionary:
```bash
python3 collect_papers.py \
  -d "/.../Relevant_papers/Undermind" --include-root \
  --paper-max 1 --no-default-outputs \
  --output-doi-dict ./doi_dict.json --stdout-format none
```

- Append updated outputs across runs (merge mode):
```bash
python3 collect_papers.py \
  -d "/.../Relevant_papers" \
  --paper-max 50 --output-write-mode append
```

- Overwrite URL dictionary after parsing exactly the `references.txt` URLs:
```bash
python3 collect_papers.py \
  -d "/.../Relevant_papers/Undermind" --include-root \
  --paper-max 2 --no-default-outputs \
  --output-url-dict ./url_dict.json --output-write-mode overwrite --stdout-format none
```

- Skip already parsed documents across runs:
```bash
python3 collect_papers.py \
  --input-doc-registry ./doc_registry.json --skip-parsed \
  --paper-max 100 --stdout-format none
```

## Identifier extraction details
- DOI: `10.\d{4,9}/[-._;()/:A-Z0-9]+` (case-insensitive)
- URLs: `https?://[^\s)] +`
- PMID: `PMID\s*:?\s*(\d{4,9})` or from `pubmed.ncbi.nlm.nih.gov/<id>`
- PMCID: `PMCID?\s*:?\s*(PMC\d+)` or from `ncbi.nlm.nih.gov/pmc/articles/PMC<id>`
- DOI URLs: from `doi.org/<doi>`

## Limitations
- Lightweight, regex-based parsing (no AST/strict parsing for BibTeX/RIS)
- PDF parsing is best-effort and may miss text-only content in complex PDFs
- `.docx` extraction uses basic XML text; complex formatting/embedded objects are ignored
- No network calls or external metadata enrichment
- DOI normalization removes neither trailing punctuation nor casing beyond pattern matching; downstream consumers may want to normalize (e.g., trim trailing `.,;`).

## Tips and troubleshooting
- Dropbox path resolution: paths may appear under `~/Library/CloudStorage/Dropbox/...` depending on macOS. The script stores resolved absolute paths.
- To keep original input paths, a future enhancement could add a `--preserve-input-paths` flag.
- If outputs unexpectedly empty, check:
  - Scope (`-d` path) and whether `--include-root` is needed
  - `--paper-max` not too small
  - `--skip-parsed` with a preloaded registry might skip intended files
  - `--no-default-outputs` disables automatic file writes unless explicit `--output-*` flags are provided

## Data contracts (JSON)
- URL dictionary (`--output-url-dict`): `{ URL: [ "file1", "file2", ... ], ... }`
  - Sidecar records: `{ URL: { doi, title, authors, year, venue, sources: [ ... ] }, ... }`
- DOI dictionary (`--output-doi-dict`): `{ DOI: [ "file1", ... ], ... }`
  - Sidecar records: `{ DOI: { title, authors, year, venue, sources: [ ... ] }, ... }`
- Document registry (`--output-doc-registry`): `{ "/abs/path/file": { kind, parsed, error, size_bytes, mtime_iso, info: { ... } }, ... }`
- Folder summaries (`--output-json`): `[ { ...FolderSummary fields... }, ... ]`

## Changelog (key features)
- Flag-only inputs via `-d/--dir/--path/--input`
- Root folder scanning via `--include-root`
- Per-document registry with cross-run skipping (`--input-doc-registry`, `--skip-parsed`)
- URL/DOI/PMID/PMCID dictionaries + sidecar metadata records
- Append/overwrite JSON output modes (`--output-write-mode`)

## License
Project-specific; add a license file if needed.
