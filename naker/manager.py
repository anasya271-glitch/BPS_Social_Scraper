# ============================================================
# NAKER SENTINEL — Data Manager Module
# Path: naker/manager.py
# Handles checkpointing, saving, merging audit files
# ============================================================

import os
import json
import csv
import re
import shutil
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger("naker.manager")


def _normalize_text(text) -> str:
    """
    Normalize text for deduplication:
    - Convert to string, strip, lowercase
    - Collapse multiple whitespace into single space
    - Remove leading/trailing whitespace
    """
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)
    text = text.strip().lower()
    text = re.sub(r'\s+', ' ', text)
    return text


class DataManager:
    """Manages data persistence: checkpoints, saves, merges, dedup."""

    def __init__(self, config: dict):
        dm_cfg = config.get("data_management", {})
        proj_cfg = config.get("project", {})

        self.output_dir = Path(proj_cfg.get("output_dir", "output"))
        self.checkpoint_dir = Path(proj_cfg.get("checkpoint_dir", "output/checkpoints"))
        self.audit_dir = Path(proj_cfg.get("audit_dir", "output/audits"))
        self.checkpoint_interval = dm_cfg.get("checkpoint_interval", 10)
        self.auto_merge = dm_cfg.get("auto_merge", True)
        self.backup_before_merge = dm_cfg.get("backup_before_merge", True)
        self.output_formats = dm_cfg.get("output_formats", ["json"])
        self.json_indent = dm_cfg.get("json_indent", 2)
        self.csv_delimiter = dm_cfg.get("csv_delimiter", ",")
        self.dedup_field = dm_cfg.get("dedup_field", "url")

        # [BUG FIX] Backup rotation limit — configurable, default 5
        self.max_backup_count = dm_cfg.get("max_backup_count", 5)

        # Ensure directories exist
        for d in [self.output_dir, self.checkpoint_dir, self.audit_dir]:
            d.mkdir(parents=True, exist_ok=True)

        self._buffer: list[dict] = []
        self._session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._seen_keys = set()
        self._article_count = 0

        # [NEW] File visited URLs persistent
        self.visited_file = self.output_dir / "visited_url_naker.txt"
        self._load_visited_urls()

    def _load_visited_urls(self):
        """Muat visited URLs dari file text agar persist lintas sesi."""
        if self.visited_file.exists():
            with open(self.visited_file, "r", encoding="utf-8") as f:
                for line in f:
                    url = line.strip()
                    if url:
                        self._seen_keys.add(url)
            logger.info(f"Memuat {len(self._seen_keys)} URL yang sudah dikunjungi dari {self.visited_file}")

    def _append_visited_url(self, url: str):
        """Simpan satu URL baru ke file text."""
        try:
            with open(self.visited_file, "a", encoding="utf-8") as f:
                f.write(f"{url}\n")
        except Exception as e:
            logger.error(f"Gagal menulis ke {self.visited_file}: {e}")

    # --- Deduplication ---

    def _dedup_key(self, article: dict) -> str:
        """
        Generate a dedup key from article.

        [BUG FIX] Normalize value (strip, lowercase, collapse whitespace)
        before using as key. This prevents false negatives from
        ' https://Example.com ' vs 'https://example.com'.
        """
        raw_value = article.get(self.dedup_field, "")
        value = _normalize_text(raw_value)

        if not value:
            # Fallback: hash normalized title + source
            title = _normalize_text(article.get("title", ""))
            source = _normalize_text(article.get("source", ""))
            combined = f"{title}|{source}"
            value = hashlib.md5(combined.encode("utf-8")).hexdigest()

        return value

    def is_duplicate(self, article: dict) -> bool:
        key = self._dedup_key(article)
        return key in self._seen_keys

    def register(self, article: dict) -> bool:
        """
        Register an article. Returns True if new, False if duplicate.
        """
        url = article.get("url")
        if not url:
            return False

        key = _normalize_text(url)
        if not key or key in self._seen_keys:
            return False

        self._seen_keys.add(key)
        self._append_visited_url(key)  # [NEW] Langsung catat ke file TXT saat ditemukan
        self._article_count += 1
        return True

    # --- Buffer & Checkpoint ---

    def add(self, article: dict):
        """Add article to buffer; auto-checkpoint if interval reached."""
        if not self.register(article):
            return

        self._buffer.append(article)
        self._article_count += 1

        if self._article_count % self.checkpoint_interval == 0:
            self.save_checkpoint()

    def add_batch(self, articles: list[dict]):
        for a in articles:
            self.add(a)

    def save_checkpoint(self):
        """
        Save current buffer to a checkpoint file.

        [BUG FIX] Atomic write — write to .tmp first, then os.replace()
        so a crash mid-write never leaves a corrupted checkpoint.
        """
        if not self._buffer:
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        cp_path = self.checkpoint_dir / f"checkpoint_{self._session_id}_{ts}.json"
        tmp_path = cp_path.with_suffix(".tmp")

        try:
            # Write to temp file first
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(
                    self._buffer, f,
                    ensure_ascii=False,
                    indent=self.json_indent,
                    default=str,  # [BUG FIX] Handle datetime & non-serializable types
                )

            # Atomic rename: tmp -> final
            os.replace(str(tmp_path), str(cp_path))

            logger.info(
                f"Checkpoint saved: {cp_path.name} ({len(self._buffer)} articles)"
            )
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
            # Clean up temp file if it exists
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass

    # --- Final Save ---

    def save_final(self, articles: Optional[list[dict]] = None, tag: str = "audit"):
        """Save final results in configured formats."""
        data = articles if articles is not None else self._buffer
        if not data:
            logger.warning("No articles to save")
            return {}

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        saved_files = {}

        if "json" in self.output_formats:
            path = self.audit_dir / f"{tag}_{self._session_id}_{ts}.json"
            tmp_path = path.with_suffix(".tmp")
            try:
                # [BUG FIX] Atomic write for final save too
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(
                        data, f,
                        ensure_ascii=False,
                        indent=self.json_indent,
                        default=str,
                    )
                os.replace(str(tmp_path), str(path))
                saved_files["json"] = str(path)
                logger.info(f"Saved JSON: {path}")
            except Exception as e:
                logger.error(f"JSON save error: {e}")
                try:
                    if tmp_path.exists():
                        tmp_path.unlink()
                except OSError:
                    pass

        if "csv" in self.output_formats:
            path = self.audit_dir / f"{tag}_{self._session_id}_{ts}.csv"
            try:
                self._save_csv(data, path)
                saved_files["csv"] = str(path)
                logger.info(f"Saved CSV: {path}")
            except Exception as e:
                logger.error(f"CSV save error: {e}")

        return saved_files

    def load_visited_urls(self) -> set:
        """Memuat daftar URL yang sudah pernah diproses dari sesi-sesi sebelumnya."""
        visited_file = self.output_dir / "visited_naker_urls.txt"
        if not visited_file.exists():
            return set()
        with open(visited_file, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())

    def save_visited_urls_delta(self, new_urls: set):
        """Menyimpan sinkronisasi URL baru ke dalam memori permanen secara aman."""
        if not new_urls:
            return
        visited_file = self.output_dir / "visited_naker_urls.txt"
        existing = self.load_visited_urls()
        combined = existing.union(new_urls)
        with open(visited_file, "w", encoding="utf-8") as f:
            for url in sorted(combined):
                f.write(f"{url}\n")

    def merge_audit_files(self, start_date: str = "", end_date: str = ""):
        """
        Menyatukan semua file audit XLSX menjadi satu Master File dengan filter tanggal.
        """
        import pandas as pd
        import re
        
        all_files = list(self.output_dir.glob("audit_naker_*.xlsx"))
        if not all_files:
            logger.warning(" [!] Tidak ada file audit ditemukan.")
            return

        filtered_files = []
        for f in all_files:
            # Ekstrak tanggal YYYYMMDD dari nama file
            match = re.search(r"(\d{8})", f.name)
            if match:
                file_date = match.group(1)
                if start_date and file_date < start_date.replace("-", ""): continue
                if end_date and file_date > end_date.replace("-", ""): continue
            filtered_files.append(f)

        if not filtered_files:
            logger.warning(f" [!] Tidak ada file dalam rentang {start_date} s/d {end_date}")
            return

        logger.info(f" [>] Menggabungkan {len(filtered_files)} file...")
        df_list = [pd.read_excel(f) for f in filtered_files]
        master_df = pd.concat(df_list, ignore_index=True).drop_duplicates(subset=['url'])
        
        output_path = self.output_dir / f"MASTER_AUDIT_NAKER_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        master_df.to_excel(output_path, index=False)
        logger.info(f" [+] Master File berhasil dibuat: {output_path}")

    def _save_csv(self, data: list[dict], path: Path):
        """Flatten and save as CSV."""
        if not data:
            return

        flat_rows = [self._flatten_dict(d) for d in data]

        # [BUG FIX] Preserve column order, deduplicate keys
        all_keys = []
        seen = set()
        for row in flat_rows:
            for k in row:
                if k not in seen:
                    all_keys.append(k)
                    seen.add(k)

        # [BUG FIX] Atomic write for CSV too
        tmp_path = path.with_suffix(".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=all_keys,
                    delimiter=self.csv_delimiter,
                    extrasaction="ignore",
                )
                writer.writeheader()
                writer.writerows(flat_rows)

            os.replace(str(tmp_path), str(path))
        except Exception as e:
            logger.error(f"CSV write error: {e}")
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass
            raise  # Re-raise so caller logs it too

    @staticmethod
    def _flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict:
        """
        Recursively flatten nested dict for CSV export.

        [BUG FIX] Convert all values to str to prevent CSV writer
        from choking on datetime, None, or other non-string types.
        """
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(DataManager._flatten_dict(v, new_key, sep))
            elif isinstance(v, (list, tuple)):
                items[new_key] = "; ".join(str(i) for i in v)
            elif v is None:
                items[new_key] = ""
            else:
                items[new_key] = str(v) if not isinstance(v, (str, int, float)) else v
        return items

    # --- Merge ---

    def merge_checkpoints(self, output_tag: str = "merged") -> dict:
        """Merge all checkpoint files into a single output."""
        cp_files = sorted(
            self.checkpoint_dir.glob(f"checkpoint_{self._session_id}_*.json")
        )
        if not cp_files:
            logger.info("No checkpoints to merge")
            return {}

        if self.backup_before_merge:
            self._backup_checkpoints(cp_files)

        all_articles = []
        seen = set()
        for cp in cp_files:
            try:
                with open(cp, "r", encoding="utf-8") as f:
                    articles = json.load(f)

                if not isinstance(articles, list):
                    logger.warning(f"Checkpoint {cp.name} is not a list, skipping")
                    continue

                for a in articles:
                    # [BUG FIX] Use normalized dedup during merge too
                    key = self._dedup_key(a)
                    if key not in seen:
                        seen.add(key)
                        all_articles.append(a)
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error in checkpoint {cp.name}: {e}")
            except Exception as e:
                logger.error(f"Error reading checkpoint {cp.name}: {e}")

        logger.info(
            f"Merged {len(cp_files)} checkpoints → {len(all_articles)} unique articles"
        )
        return self.save_final(all_articles, tag=output_tag)

    def _backup_checkpoints(self, files: list[Path]):
        """
        Backup checkpoint files before merge.

        [BUG FIX] Rotate old backups — keep only max_backup_count
        most recent backup directories to prevent unbounded disk usage.
        """
        backup_dir = self.checkpoint_dir / "backup"
        backup_dir.mkdir(exist_ok=True)

        # Create timestamped subdirectory for this backup batch
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_dir = backup_dir / f"batch_{ts}"
        batch_dir.mkdir(exist_ok=True)

        for f in files:
            try:
                shutil.copy2(f, batch_dir / f.name)
            except OSError as e:
                logger.warning(f"Failed to backup {f.name}: {e}")

        logger.info(f"Backed up {len(files)} checkpoint files to {batch_dir}")

        # [BUG FIX] Rotate — remove oldest backup batches if over limit
        self._rotate_backups(backup_dir)

    def _rotate_backups(self, backup_dir: Path):
        """Keep only the N most recent backup batches."""
        batches = sorted(
            [d for d in backup_dir.iterdir() if d.is_dir() and d.name.startswith("batch_")],
            key=lambda p: p.name,
        )

        while len(batches) > self.max_backup_count:
            oldest = batches.pop(0)
            try:
                shutil.rmtree(oldest)
                logger.debug(f"Removed old backup batch: {oldest.name}")
            except OSError as e:
                logger.warning(f"Could not remove old backup {oldest.name}: {e}")

    # --- Load Previous Session ---

    def load_existing(self, path: str) -> list[dict]:
        """Load articles from a previous audit file to resume/extend."""
        p = Path(path)
        if not p.exists():
            logger.warning(f"File not found: {path}")
            return []

        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not isinstance(data, list):
                logger.warning(f"Data in {path} is not a list, returning empty")
                return []

            # Register all for dedup
            registered = 0
            for a in data:
                if self.register(a):
                    registered += 1

            logger.info(
                f"Loaded {len(data)} articles from {path} "
                f"({registered} new, {len(data) - registered} already seen)"
            )
            return data
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error loading {path}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error loading {path}: {e}")
            return []

    # --- Stats ---

    @property
    def stats(self) -> dict:
        return {
            "session_id": self._session_id,
            "total_processed": self._article_count,
            "buffer_size": len(self._buffer),
            "unique_registered": len(self._seen_keys),
            "output_dir": str(self.output_dir),
        }