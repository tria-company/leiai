"""
extractor.py — Dispatcher: decide qual extrator usar com base na extensão do arquivo.
"""

from pathlib import Path

from .schemas import empty_payload
from .xlsx_extractor import extract_xlsx
from .pdf_extractor import extract_pdf


def extract_document(filepath: str, api_key: str = '', rate_limiter=None, error_log: list = None) -> dict:
    """Extrai payload do documento independente do formato."""
    ext = Path(filepath).suffix.lower()
    try:
        if ext == '.xlsx':
            return extract_xlsx(filepath)
        elif ext == '.pdf':
            return extract_pdf(filepath, api_key, rate_limiter, error_log)
        else:
            if error_log is not None:
                error_log.append({
                    'arquivo': filepath,
                    'erro': f'Formato não suportado: {ext}',
                    'tipo': 'formato',
                })
            return empty_payload()
    except Exception as e:
        msg = f"Falha ao extrair: {e}"
        print(f"  [ERRO] {msg}", flush=True)
        if error_log is not None:
            error_log.append({
                'arquivo': filepath,
                'erro': msg,
                'tipo': 'extracao',
            })
        return empty_payload()
