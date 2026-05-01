"""Run this script to import all new PDF files that are not yet in the cache."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
os.chdir(os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

import database as db; db.init_db()
import pdf_parser as pp
from pathlib import Path

ROOT = r'C:\Users\Bruno Barbosa\OneDrive - Amazing Evolution, S.A\Sales, Marketing & Revenue - Relatórios Hotéis'
root = Path(ROOT)
HOTELS = {
    '1905 Zinos Palace', 'Hotel da Graciosa', 'Land of Alandroal',
    'Luster', 'Palácio Sta. Catarina', 'Sleep and Nature',
    'Solar dos Cantos', 'The Shipyard Angra',
}
GLOBS = [
    '*/Relatórios Diários*/**/150. Manager*.pdf',
    '*/Relatórios diários*/**/150. Manager*.pdf',
    '*/Relatórios Diários*/**/101. Saldos*.pdf',
    '*/Relatórios diários*/**/101. Saldos*.pdf',
    '*/Relatórios Diários*/**/Report Manager.pdf',
    '*/Relatórios diários*/**/Report Manager.pdf',
]

seen = set()
new_files = []
for g in GLOBS:
    for f in root.glob(g):
        hotel = f.relative_to(root).parts[0]
        if hotel in HOTELS and str(f) not in seen:
            seen.add(str(f))
            if not db.is_file_unchanged(str(f), f.stat().st_mtime):
                new_files.append(str(f))

print(f'PDFs novos para importar: {len(new_files)}')
ok = err = 0
for fpath in sorted(new_files):
    result = pp.import_pdf_file(fpath, force=True)
    hotel = Path(fpath).relative_to(root).parts[0]
    date = pp._date_from_path(fpath)
    name = Path(fpath).name[:40]
    if result > 0:
        ok += 1
        print(f'  OK  | {hotel[:25]} | {date} | {name}')
    else:
        err += 1
        print(f'  ERR | {hotel[:25]} | {date} | {name}')

print(f'\nTotal: {ok} OK, {err} vazios/erro')
