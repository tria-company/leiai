"""
helpers.py — Funções utilitárias: normalização, busca de arquivos, formatação.
"""

import os
import re
import shutil
import unicodedata
from pathlib import Path
from datetime import datetime, date

from .config import IGNORE_FILES, IGNORE_PREFIXES, MESES, DIAS_SEMANA


# ---------------------------------------------------------------------------
# Normalização e filtros
# ---------------------------------------------------------------------------

def normalize(text: str) -> str:
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in nfkd if not unicodedata.combining(c)).lower()


def is_target(filename: str) -> bool:
    norm = normalize(filename)
    return 'formulario' in norm or 'analise' in norm or 'analis' in norm


def priority(filename: str) -> tuple:
    norm = normalize(filename)
    ext = Path(filename).suffix.lower()
    if 'formulario' in norm:
        return (0, {'.xlsx': 1, '.pdf': 2}.get(ext, 3))
    if 'analise' in norm or 'analis' in norm:
        return (1, {'.pdf': 1, '.xlsx': 2}.get(ext, 3))
    return (99, 99)


def is_user_file(filename: str) -> bool:
    if filename in IGNORE_FILES:
        return False
    if any(filename.startswith(p) for p in IGNORE_PREFIXES):
        return False
    return True


# ---------------------------------------------------------------------------
# Extração de metadados do caminho
# ---------------------------------------------------------------------------

def extract_month_prefix(rel_path: str) -> str:
    """Extrai o nome do mês da pasta (para prefixo do consolidado)."""
    parts = Path(rel_path).parts
    if len(parts) < 2:
        return ''
    folder = parts[1]  # "JANEIRO 2025" ou "FEVEREIRO 2025"
    return re.sub(r'\s+20\d{2}$', '', folder).strip()


def extract_pessoa(rel_path: str, filename: str = '') -> str:
    """Extrai o nome da pessoa do caminho ou do nome do arquivo."""
    parts = Path(rel_path).parts

    # 1) Tenta subfolder (parts[2])
    if len(parts) >= 3:
        subfolder = parts[2]
        cleaned = re.sub(r'\s*-?\s*20\d{2}\s*$', '', subfolder).strip()
        words = cleaned.split()
        name_words = [w for w in words if w.upper() not in MESES and w != '-']
        if name_words:
            candidate = ' '.join(name_words).strip()
            if re.match(r'^[A-Za-zÀ-ÿ\s]+$', candidate):
                return candidate

    # 2) Tenta extrair do nome do arquivo
    if filename:
        stem = Path(filename).stem
        cleaned = re.sub(r'^(C[oó]pia\s+de\s+)+', '', stem, flags=re.IGNORECASE)
        cleaned = re.sub(r'^(FORMUL[AÁ]RIO|AN[AÁ]LISE)\s+', '', cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r'\s+EXPRES\b.*$', '', cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r'\s+20\d{2}.*$', '', cleaned).strip()
        cleaned = re.sub(r'\s+PDF$', '', cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r'\s*\.xlsx\b.*$', '', cleaned, flags=re.IGNORECASE).strip()
        if cleaned and re.match(r'^[A-Za-zÀ-ÿ\s]+$', cleaned) and len(cleaned) > 1:
            return cleaned

    return ''


# ---------------------------------------------------------------------------
# Cópia de arquivos
# ---------------------------------------------------------------------------

def safe_copy(src: str, dest_dir: str, dest_name: str) -> str:
    """Copia arquivo, renomeando com _1, _2… se já existir. Retorna o nome real usado."""
    dest = os.path.join(dest_dir, dest_name)
    if not os.path.exists(dest):
        shutil.copy2(src, dest)
        return dest_name
    stem = Path(dest_name).stem
    suffix = Path(dest_name).suffix
    counter = 1
    while True:
        actual_name = f"{stem}_{counter}{suffix}"
        dest = os.path.join(dest_dir, actual_name)
        if not os.path.exists(dest):
            shutil.copy2(src, dest)
            return actual_name
        counter += 1


# ---------------------------------------------------------------------------
# Formatação de valores
# ---------------------------------------------------------------------------

def excel_serial_to_date(serial) -> str:
    """Converte serial number do Excel (ex: 45569) para dd/mm/yyyy."""
    if isinstance(serial, (int, float)) and 40000 < serial < 60000:
        from datetime import timedelta
        base = datetime(1899, 12, 30)
        dt = base + timedelta(days=int(serial))
        return dt.strftime('%d/%m/%Y')
    return None


def fmt_val(v, as_weekday=False) -> str:
    """Converte valor de célula para string limpa."""
    if v is None:
        return ''
    if isinstance(v, datetime):
        if as_weekday:
            return DIAS_SEMANA.get(v.weekday(), '') + ', ' + v.strftime('%d/%m/%Y')
        return v.strftime('%d/%m/%Y') if v.hour == 0 and v.minute == 0 else v.strftime('%H:%M')
    if isinstance(v, date):
        if as_weekday:
            return DIAS_SEMANA.get(v.weekday(), '') + ', ' + v.strftime('%d/%m/%Y')
        return v.strftime('%d/%m/%Y')
    converted = excel_serial_to_date(v)
    if converted:
        if as_weekday:
            from datetime import timedelta
            base = datetime(1899, 12, 30)
            dt = base + timedelta(days=int(v))
            return DIAS_SEMANA.get(dt.weekday(), '') + ', ' + converted
        return converted
    s = str(v).strip()
    return s


def fmt_money(v) -> str:
    """Formata valor numérico como R$."""
    if v is None:
        return ''
    if isinstance(v, (int, float)):
        return f"R$ {v:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    return str(v).strip()
