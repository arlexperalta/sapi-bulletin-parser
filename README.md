# SAPI Bulletin Parser

Extract structured data from Venezuelan Intellectual Property bulletins (SAPI — Servicio Autónomo de la Propiedad Intelectual).

**Used in production to process 740K+ records from 177 government PDF bulletins spanning 1953-2024.**

## The problem

Venezuela's intellectual property records — trademarks, patents, and legal publications — are locked inside PDF bulletins published by the government since 1953. Each bulletin contains hundreds to thousands of entries in inconsistent formats that have changed across decades. No structured database exists.

## What this does

A two-phase pipeline that transforms raw PDF bulletins into clean, structured JSON:

```
PDF bulletin → [Phase 1: extract_pdf.py] → raw text + images
                                              ↓
raw text     → [Phase 2: parse_entries.py] → structured JSON entries
```

### Phase 1: PDF extraction (`extract_pdf.py`)

Reads PDF files using `pymupdf` and outputs:
- Full text per volume (tomo)
- Text per page (for image linking)
- Extracted images (filtered by size: min 50x50px, 500+ bytes)
- Metadata JSON (page count, character count, image stats)

### Phase 2: Structured parsing (`parse_entries.py`)

Parses raw text into structured entries using regex-based extraction. Handles multiple format variations across 70 years of bulletins:

- **Inscriptions** (trademark registrations with holder, class, description)
- **Resolutions** (denied/observed applications with legal reasoning)
- **Registry tables** (tabular format with columns for application, class, holder)
- **WIPO patents** (international patent publications)

Each entry is extracted with:

```json
{
  "nroSolicitud": "2003-018041",
  "fechaInscripcion": "2003-12-08",
  "nombreMarca": "VM23",
  "titularNombre": "VOLVO DO BRASIL VEICULOS LTDA",
  "titularDomicilio": "AV BRASIL",
  "titularPais": "BRASIL",
  "clase": "12",
  "distingue": "camiones.",
  "tramitante": "MONTIEL SALAS MARIA ANA",
  "seccion": "MARCAS_SOLICITADAS_OPOSICION"
}
```

## Challenges solved

- **Format drift:** Bulletin layouts changed significantly across decades (1953-2024). The parser handles 6+ distinct format variations.
- **OCR degradation:** Older bulletins have character degradation, broken encoding, and inconsistent spacing.
- **Multi-line fields:** Holder names, addresses, and descriptions often span multiple lines with no clear delimiter.
- **Page break noise:** Headers, footers, and page numbers interrupt entries mid-field.
- **Section detection:** Each bulletin contains multiple sections (granted, denied, observed, WIPO) with different entry formats.

## Usage

### Phase 1: Extract text and images from PDF

```bash
python extract_pdf.py /path/to/bulletin.pdf /path/to/output/
```

### Phase 2: Parse extracted text into structured entries

```bash
python parse_entries.py /path/to/output/
```

Output: `entradas_ia_{bulletin}_completo.json`

## Requirements

```
pip install pymupdf
```

Phase 2 (`parse_entries.py`) uses only the Python standard library — no external dependencies.

## Production results

| Metric | Value |
|--------|-------|
| Bulletins processed | 177 |
| Total entries extracted | 740,062 |
| Images linked to entries | 226,965 |
| Section accuracy | 88.4% |
| Date range | 1953 - 2024 |

## Examples

See the `examples/` directory:
- `examples/input/sample_page.txt` — raw text from a bulletin page
- `examples/output/sample_entries.json` — structured entries extracted from a bulletin

## Context

This parser is part of [Vindex](https://github.com/arlexperalta), a platform that makes 70 years of Venezuelan intellectual property records searchable for the first time. The extraction pipeline runs against the complete archive of SAPI bulletins — public government documents available from the Venezuelan IP office.

## License

MIT
