"""
runner.py — Orquestrador principal com 3 fases:
  1. Coleta de arquivos (single-thread)
  2. Extração em paralelo (ThreadPoolExecutor)
  3. Gravação de saídas (single-thread)
"""

import os
import re
import csv
import json
import time
import threading
from pathlib import Path
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import WorkspaceConfig, load_openai_key, MAX_WORKERS, RPM_LIMIT
from .schemas import (
    CSV_COLUMNS, INTERNAL_KEYS,
    empty_payload, payload_to_csv_row, payload_to_json_row,
)
from .helpers import (
    is_user_file, is_target, priority,
    extract_month_prefix, extract_pessoa, safe_copy,
)
from .extractor import extract_document
from .rate_limiter import RateLimiter


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class FileTask:
    src_path: str
    filename: str
    rel_path: str
    pessoa: str
    month_prefix: str
    folder_url: str


class ResultCollector:
    """Acumulador thread-safe de resultados."""

    def __init__(self):
        self._lock = threading.Lock()
        self.log_rows: list = []
        self.extract_rows: list = []
        self.error_log: list = []
        self.docs_erros: list = []

    def add_log(self, row: list):
        with self._lock:
            self.log_rows.append(row)

    def add_extract(self, payload: dict):
        with self._lock:
            self.extract_rows.append(payload)

    def add_error(self, err: dict):
        with self._lock:
            self.error_log.append(err)

    def add_doc_erro(self, doc: dict):
        with self._lock:
            self.docs_erros.append(doc)

    def error_count(self) -> int:
        with self._lock:
            return len(self.error_log)


# ---------------------------------------------------------------------------
# Fase 1: Coleta
# ---------------------------------------------------------------------------

def _collect_files(cfg: WorkspaceConfig) -> tuple[list[FileTask], list[list]]:
    """Varre diretórios e coleta arquivos-alvo. Retorna (tasks, missed_log_rows)."""
    tasks = []
    missed = []

    for root, dirs, files in os.walk(cfg.base_dir):
        dirs[:] = sorted(d for d in dirs if not d.startswith('__'))

        user_files = [f for f in files if is_user_file(f)]
        if not user_files:
            continue

        rel_path = os.path.relpath(root, cfg.workspace)

        if rel_path == os.path.basename(cfg.base_dir):
            continue

        month_prefix = extract_month_prefix(rel_path)
        if not month_prefix:
            continue

        folder_url = '.' + os.sep + rel_path
        target_files = sorted(
            [f for f in user_files if is_target(f)],
            key=priority,
        )

        if target_files:
            best = target_files[0]
            src_path = os.path.join(root, best)
            pessoa = extract_pessoa(rel_path, best)
            tasks.append(FileTask(
                src_path=src_path,
                filename=best,
                rel_path=rel_path,
                pessoa=pessoa,
                month_prefix=month_prefix,
                folder_url=folder_url,
            ))
        else:
            pessoa = extract_pessoa(rel_path)
            missed.append([pessoa, folder_url, '', 'XXX'])

    return tasks, missed


def _collect_from_folder(files_dir: str, cfg: WorkspaceConfig) -> list[FileTask]:
    """Coleta arquivos .xlsx/.pdf de uma pasta específica (sem varredura recursiva)."""
    SUPPORTED = {'.xlsx', '.pdf'}
    files_dir = os.path.abspath(files_dir)
    tasks = []

    for fname in sorted(os.listdir(files_dir)):
        fpath = os.path.join(files_dir, fname)
        if not os.path.isfile(fpath):
            continue
        if not is_user_file(fname):
            continue
        ext = os.path.splitext(fname)[1].lower()
        if ext not in SUPPORTED:
            continue

        rel_path = os.path.relpath(files_dir, cfg.workspace)
        pessoa = extract_pessoa(rel_path, fname)
        month_prefix = extract_month_prefix(rel_path)

        tasks.append(FileTask(
            src_path=fpath,
            filename=fname,
            rel_path=rel_path,
            pessoa=pessoa or Path(fname).stem,
            month_prefix=month_prefix,
            folder_url='.' + os.sep + rel_path,
        ))

    return tasks


# ---------------------------------------------------------------------------
# Fase 2: Processamento individual (executado em thread)
# ---------------------------------------------------------------------------

def _process_one(
    task: FileTask,
    api_key: str,
    rate_limiter: RateLimiter,
    collector: ResultCollector,
    consolidated_dir: str,
    copy_lock: threading.Lock,
):
    """Processa um único arquivo: extrai dados, copia para consolidados."""
    local_errors = []

    try:
        t0 = time.perf_counter()
        print(f"  Extraindo: {task.filename} ...", flush=True)
        payload = extract_document(task.src_path, api_key, rate_limiter, local_errors)
        elapsed = time.perf_counter() - t0
        if elapsed > 5:
            print(f"    [LENTO] {task.filename} demorou {elapsed:.1f}s", flush=True)
        payload['pessoa'] = task.pessoa
        payload['pasta_origem'] = task.folder_url

        # Registra erros locais no collector
        for err in local_errors:
            collector.add_error(err)

        # Verifica se a extração teve resultado útil
        filled = sum(
            1 for k in INTERNAL_KEYS
            if k not in ('arquivo_origem', 'pessoa', 'pasta_origem')
            and payload.get(k, '')
        )
        had_error = len(local_errors) > 0
        if had_error or filled == 0:
            collector.add_doc_erro({
                'arquivo_original': task.filename,
                'caminho_completo': task.src_path,
                'pasta': task.folder_url,
                'pessoa': task.pessoa,
                'campos_extraidos': filled,
                'motivo': local_errors[-1]['erro'] if had_error else 'Nenhum campo extraído',
            })
            if filled == 0:
                collector.add_error({
                    'arquivo': task.src_path,
                    'erro': f'Nenhum campo extraído (0/{len(INTERNAL_KEYS)} campos)',
                    'tipo': 'vazio',
                })
            print(f"    [AVISO] Apenas {filled} campos extraídos de {task.filename}", flush=True)

        # --- Monta nome do arquivo consolidado ---
        def _safe_field(val):
            s = str(val).strip() if val else ''
            s = s.replace('\n', ' ').replace('\r', ' ')
            s = re.sub(r'\s{2,}', ' ', s)
            return s[:50] or 'NA'

        _r = _safe_field(payload.get('ressarcimento', ''))
        _d = _safe_field(payload.get('data_fato', ''))
        _n = _safe_field(payload.get('protocolo_segurado', ''))
        _a = _safe_field(payload.get('analista_responsavel_segurado', ''))
        _s = _safe_field(task.pessoa)
        _t = _safe_field(payload.get('cobertura_terceiros', ''))
        ext = Path(task.filename).suffix
        new_name = f"{_r} - {_d} - {_n} - {_a} - {_s} - {_t}{ext}"
        new_name = re.sub(r'[\\/:*?"<>|\r\n]', '-', new_name)
        if len(new_name) > 200:
            new_name = new_name[:195] + '...' + ext

        # safe_copy precisa de lock porque threads podem criar nomes iguais
        with copy_lock:
            actual_name = safe_copy(task.src_path, consolidated_dir, new_name)

        payload['arquivo_origem'] = actual_name
        collector.add_log([task.pessoa, task.folder_url, task.filename, 'ENCONTRADO'])
        collector.add_extract(payload)

    except Exception as e:
        msg = f"Erro fatal processando {task.filename}: {e}"
        print(f"  [ERRO] {msg}", flush=True)
        collector.add_error({
            'arquivo': task.src_path,
            'erro': msg,
            'tipo': 'fatal',
        })
        collector.add_doc_erro({
            'arquivo_original': task.filename,
            'caminho_completo': task.src_path,
            'pasta': task.folder_url,
            'pessoa': task.pessoa,
            'campos_extraidos': 0,
            'motivo': msg,
        })
        collector.add_log([task.pessoa, task.folder_url, task.filename, 'ERRO'])


# ---------------------------------------------------------------------------
# Fase 3: Saída
# ---------------------------------------------------------------------------

def _write_outputs(cfg: WorkspaceConfig, collector: ResultCollector):
    """Grava CSV, JSON, logs de busca e erros."""

    # --- Log de busca ---
    with open(cfg.log_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['Pessoa', 'folder_url', 'file_name', 'status'])
        writer.writerows(collector.log_rows)

    # --- CSV de extração ---
    extract_csv = cfg.extract_csv
    try:
        with open(extract_csv, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in collector.extract_rows:
                writer.writerow(payload_to_csv_row(row))
    except PermissionError:
        alt = extract_csv.replace('.csv', '_v2.csv')
        print(f"  [AVISO] CSV aberto, salvando como: {alt}")
        with open(alt, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in collector.extract_rows:
                writer.writerow(payload_to_csv_row(row))
        extract_csv = alt

    # --- JSON de extração ---
    json_rows = [payload_to_json_row(row) for row in collector.extract_rows]
    with open(cfg.extract_json, 'w', encoding='utf-8') as f:
        json.dump(json_rows, f, ensure_ascii=False, indent=2)

    # --- Log de erros ---
    with open(cfg.error_log_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=['arquivo', 'tipo', 'erro'], quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for err in collector.error_log:
            writer.writerow(err)

    # --- Docs com erro ---
    with open(cfg.docs_erros_path, 'w', encoding='utf-8') as f:
        json.dump(collector.docs_erros, f, ensure_ascii=False, indent=2)

    return extract_csv


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(year: str = '2025', max_workers: int = MAX_WORKERS, rpm: int = RPM_LIMIT,
         files_dir: str | None = None):
    workspace = str(Path(__file__).resolve().parent.parent)
    cfg = WorkspaceConfig(workspace=workspace, year=year)

    # Carregar API key para OCR
    api_key = load_openai_key(workspace)
    if not api_key:
        print("[AVISO] OPENAI_API_KEY não encontrada — OCR de PDFs de imagem não funcionará.")

    # Rate limiter para chamadas OCR
    rate_limiter = RateLimiter(rpm=rpm)

    # Limpa consolidados de execuções anteriores
    if os.path.exists(cfg.consolidated):
        for f in os.listdir(cfg.consolidated):
            fp = os.path.join(cfg.consolidated, f)
            try:
                if os.path.isfile(fp):
                    os.remove(fp)
            except OSError:
                pass
    os.makedirs(cfg.consolidated, exist_ok=True)

    # ===================== FASE 1: Coleta =====================
    print("=" * 60)
    missed_rows = []

    if files_dir:
        print(f"FASE 1: Coletando arquivos de: {files_dir}")
        print("=" * 60)
        if not os.path.isdir(files_dir):
            print(f"[ERRO] Pasta não encontrada: {files_dir}")
            return
        tasks = _collect_from_folder(files_dir, cfg)
        print(f"  Encontrados: {len(tasks)} arquivos (.xlsx/.pdf)")
    else:
        print("FASE 1: Coletando arquivos (varredura completa)...")
        print("=" * 60)
        tasks, missed_rows = _collect_files(cfg)
        print(f"  Encontrados: {len(tasks)} arquivos-alvo")
        print(f"  Pastas sem alvo: {len(missed_rows)}")

    if not tasks:
        print("Nenhum arquivo para processar.")
        return

    # ===================== FASE 2: Extração paralela =====================
    print()
    print("=" * 60)
    print(f"FASE 2: Extraindo em paralelo ({max_workers} workers)...")
    print("=" * 60)

    collector = ResultCollector()
    # Adiciona pastas sem arquivo-alvo ao log
    for row in missed_rows:
        collector.add_log(row)

    copy_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_one, task, api_key, rate_limiter, collector, cfg.consolidated, copy_lock
            ): task
            for task in tasks
        }
        done = 0
        total = len(futures)
        for future in as_completed(futures):
            done += 1
            task = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"  [ERRO THREAD] {task.filename}: {e}", flush=True)
            if done % 10 == 0 or done == total:
                print(f"  Progresso: {done}/{total}", flush=True)

    # ===================== FASE 3: Saída =====================
    print()
    print("=" * 60)
    print("FASE 3: Gravando saídas...")
    print("=" * 60)

    extract_csv = _write_outputs(cfg, collector)

    # Resumo
    found = sum(1 for r in collector.log_rows if r[3] == 'ENCONTRADO')
    not_found = sum(1 for r in collector.log_rows if r[3] == 'XXX')

    print(f"\nConcluido!", flush=True)
    print(f"  Pastas processadas            : {len(collector.log_rows)}")
    print(f"  Arquivos encontrados e copiados: {found}")
    print(f"  Documentos com dados extraidos : {len(collector.extract_rows)}")
    print(f"  Pastas sem arquivo-alvo        : {not_found}")
    print(f"  Erros encontrados              : {len(collector.error_log)}")
    print(f"  Docs com falha                 : {len(collector.docs_erros)}")
    print(f"  Log       -> {cfg.log_path}")
    print(f"  CSV       -> {extract_csv}")
    print(f"  JSON      -> {cfg.extract_json}")
    print(f"  Erros     -> {cfg.error_log_path}")
    print(f"  Docs erro -> {cfg.docs_erros_path}")
    print(f"  Docs      -> {cfg.consolidated}")
