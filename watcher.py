"""
File-system watcher: detects new/modified Excel files in the OneDrive hotel
folders and triggers a background import.
"""

import logging
import threading
import time

from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent
from watchdog.observers import Observer

import parser as hp
import pdf_parser as pp
import otb_parser as op

logger = logging.getLogger(__name__)

_import_lock = threading.Lock()
_pending: set[str] = set()
_pending_lock = threading.Lock()


def _debounced_import():
    """Wait a moment, then drain the pending set and import each file."""
    time.sleep(3)  # debounce — OneDrive often fires multiple events per save
    with _pending_lock:
        files = list(_pending)
        _pending.clear()

    for path in files:
        with _import_lock:
            if hp.is_daily_report(path):
                logger.info("Importing changed Excel: %s", path)
                hp.import_file(path)
            elif pp.is_pdf_report(path):
                logger.info("Importing changed PDF: %s", path)
                pp.import_pdf_file(path)
            elif op.is_otb_report(path):
                logger.info("OTB change detected: %s", path)
                hotel = op._hotel_name_from_path(path)
                latest = op._find_latest_otb_per_hotel().get(hotel)
                if latest:
                    logger.info("Importing latest OTB for %s: %s", hotel, latest)
                    op.import_otb_for_hotel(hotel, latest, force=True)


class _HotelEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            self._schedule(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._schedule(event.src_path)

    def _schedule(self, path: str):
        if not hp.is_daily_report(path) and not pp.is_pdf_report(path) and not op.is_otb_report(path):
            return
        with _pending_lock:
            already_pending = bool(_pending)
            _pending.add(path)
        if not already_pending:
            threading.Thread(target=_debounced_import, daemon=True).start()


_observer: Observer | None = None


def start(root_path: str):
    global _observer
    if _observer and _observer.is_alive():
        return
    _observer = Observer()
    _observer.schedule(_HotelEventHandler(), root_path, recursive=True)
    _observer.start()
    logger.info("Watching for changes in: %s", root_path)


def stop():
    global _observer
    if _observer:
        _observer.stop()
        _observer.join()
        _observer = None
