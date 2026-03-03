"""
Entry point: python -m leiai_backend_v2

Uso:
    python -m leiai_backend_v2                          # padrões (2024, 8 workers, 30 RPM)
    python -m leiai_backend_v2 --year 2024 --workers 4  # customizado
    python -m leiai_backend_v2 --workers 1              # sequencial (debug)
    python -m leiai_backend_v2 --files ./minha_pasta     # processa apenas arquivos da pasta
"""

import argparse
from .runner import main


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extração paralela de formulários de sinistro')
    parser.add_argument('--year', default='2024', help='Ano a processar (default: 2024)')
    parser.add_argument('--workers', type=int, default=8, help='Threads paralelas (default: 8)')
    parser.add_argument('--rpm', type=int, default=30, help='Rate limit OCR requests/min (default: 30)')
    parser.add_argument('--files', default=None, metavar='PASTA',
                        help='Pasta com arquivos específicos a processar (ignora varredura completa)')

    args = parser.parse_args()
    main(year=args.year, max_workers=args.workers, rpm=args.rpm, files_dir=args.files)
