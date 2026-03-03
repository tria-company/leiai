"""
config.py — Constantes, paths e configuração de runtime.
"""

import os
import re
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Constantes de filtragem
# ---------------------------------------------------------------------------
IGNORE_FILES = {'.DS_Store', 'Thumbs.db', 'desktop.ini', '.gitkeep'}
IGNORE_PREFIXES = ('__', '~$')

MESES = {
    'JANEIRO', 'FEVEREIRO', 'MARCO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO',
    'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO',
}

DIAS_SEMANA = {
    0: 'segunda-feira', 1: 'terça-feira', 2: 'quarta-feira',
    3: 'quinta-feira', 4: 'sexta-feira', 5: 'sábado', 6: 'domingo',
}

# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------
MIN_TEXT_LEN = 50  # Abaixo disso, PDF é considerado imagem → OCR

# Stop universal: headers de seção do formulário (previne regex "runaway" com DOTALL)
_STOP = (
    r'AN[AÁ]LISE\s+DE\s+(?:COBERTURA|FRANQUIA)|IDENTIFICA[CÇ][AÃ]O\s+D[OE]'
    r'|ASSIST[EÊ]NCIA\s+24|CONSULTA\s+DETRAN|AN[AÁ]LISE\s+E\s+DETALHAMENTO'
    r'|PONTOS\s+A\s+EXALTAR|RESUMO\s+D[OE]|PARECER\s+[AÀ]\s+REGULAGEM'
    r'|CONCLUS[AÃ]O\s+DA\s+AN[AÁ]LISE|DANOS\s+VE[IÍ]CULO'
    r'|ANALISTA\s+RESPONS|SINISTRO\s+ABERTO|OBSERVA[CÇ][OÕ]ES'
    r'|CASO\s+DE\s+IDENTIF'
)

# ---------------------------------------------------------------------------
# Paralelismo e OCR
# ---------------------------------------------------------------------------
MAX_WORKERS = 8
RPM_LIMIT = 30
OCR_MODEL = 'gpt-4o-mini'
OCR_MAX_PAGES = 3
OCR_ZOOM_FACTOR = 1.5


# ---------------------------------------------------------------------------
# Workspace paths
# ---------------------------------------------------------------------------
@dataclass
class WorkspaceConfig:
    workspace: str
    year: str = '2025'

    @property
    def base_dir(self) -> str:
        return os.path.join(self.workspace, self.year)

    @property
    def consolidated(self) -> str:
        return os.path.join(self.base_dir, '__CONSOLIDADOS_v2')

    @property
    def log_path(self) -> str:
        return os.path.join(self.base_dir, f'__log_de_busca_{self.year}.csv')

    @property
    def extract_csv(self) -> str:
        return os.path.join(self.base_dir, f'__extracao_sinistros_{self.year}.csv')

    @property
    def extract_json(self) -> str:
        return os.path.join(self.base_dir, f'__extracao_sinistros_{self.year}.json')

    @property
    def error_log_path(self) -> str:
        return os.path.join(self.base_dir, f'__log_erros_{self.year}.csv')

    @property
    def docs_erros_path(self) -> str:
        return os.path.join(self.base_dir, f'__docs_erros_{self.year}.json')


# ---------------------------------------------------------------------------
# API Key
# ---------------------------------------------------------------------------
def load_openai_key(workspace: str) -> str:
    """Carrega OPENAI_API_KEY do arquivo leiai/.env relativo ao workspace."""
    env_path = os.path.join(workspace, 'leiai', '.env')
    if os.path.exists(env_path):
        with open(env_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('OPENAI_API_KEY='):
                    return line.split('=', 1)[1].strip()
    return ''
