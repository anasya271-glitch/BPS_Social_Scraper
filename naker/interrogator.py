# ============================================================
# NAKER SENTINEL — AI Interrogator Module (Bug-Fixed)
# Path: naker/interrogator.py
# Handles AI-powered article analysis via Ollama/Llama
# ============================================================

import json
import asyncio
import logging
import random
import re
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger("naker.interrogator")


class AIInterrogator:
    """Interrogates articles using local LLM (Ollama) for deep analysis."""

    def __init__(self, config: dict):
        intg_cfg = config.get("interrogation", {})
        self.provider = intg_cfg.get("provider", "ollama")
        self.model = intg_cfg.get("model", "llama3.1:8b")
        self.base_url = intg_cfg.get("base_url", "http://localhost:11434").rstrip("/")
        self.timeout = intg_cfg.get("timeout", 120)
        self.connect_timeout = intg_cfg.get("connect_timeout", 10)
        self.max_retries = intg_cfg.get("max_retries", 2)
        self.temperature = intg_cfg.get("temperature", 0.3)
        self.max_tokens = intg_cfg.get("max_tokens", 2048)
        self.batch_size = intg_cfg.get("batch_size", 5)
        self.max_content_length = intg_cfg.get("max_content_length", 6000)

        # Load prompt template
        prompt_dir = Path(intg_cfg.get("prompt_dir", "naker/prompts"))
        prompt_file = intg_cfg.get("analysis_prompt", "analysis.txt")
        self.prompt_template = self._load_prompt(prompt_dir / prompt_file)

        # FIX [Race Condition]: asyncio.Lock to prevent duplicate sessions
        self._session = None
        self._session_lock = asyncio.Lock()

        self._stats = {
            "total_requests": 0,
            "successful": 0,
            "failed": 0,
            "total_time": 0.0,
            "parse_errors": 0,
        }

    # --- Context manager support ---
    async def __aenter__(self):
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # --- Prompt loading ---
    def _load_prompt(self, path: Path) -> str:
        """Load prompt template from file."""
        try:
            if path.exists():
                text = path.read_text(encoding="utf-8")
                logger.info(f"Loaded prompt template from {path}")
                return text
            else:
                logger.warning(f"Prompt file not found: {path}, using default")
                return self._default_prompt()
        except Exception as e:
            logger.error(f"Error loading prompt: {e}")
            return self._default_prompt()

    @staticmethod
    def _default_prompt() -> str:
        return (
            "Analisis artikel berita berikut terkait ketenagakerjaan di Bandung.\n"
            "Judul: {title}\nSumber: {source}\nTanggal: {published_date}\n"
            "URL: {url}\n\nKonten:\n{content}\n\n"
            "Hasilkan analisis dalam format JSON dengan field: ringkasan, "
            "relevansi_ketenagakerjaan, kategori, entitas, dampak_ketenagakerjaan, "
            "indikator_bps, timeline, metadata_analisis."
        )

    # --- Session management (FIX: race-condition-safe) ---
    async def _ensure_session(self):
        """Lazy-init aiohttp session — guarded by lock to prevent race condition."""
        if self._session and not self._session.closed:
            return
        async with self._session_lock:
            # Double-check after acquiring lock
            if self._session and not self._session.closed:
                return
            import aiohttp

            timeout = aiohttp.ClientTimeout(
                total=self.timeout,
                connect=self.connect_timeout,
            )
            self._session = aiohttp.ClientSession(timeout=timeout)
            logger.debug("Created new aiohttp session for Ollama")

    async def close(self):
        """Close HTTP session safely."""
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None
                logger.debug("Closed aiohttp session")

    # --- Prompt building ---
    def _build_prompt(self, article: dict) -> str:
        """Fill prompt template with article data — with sanitization."""
        content = article.get("content", "") or ""
        # Truncate overly long content to avoid token limits
        max_len = self.max_content_length
        if len(content) > max_len:
            content = content[:max_len] + "\n\n[...konten dipotong karena terlalu panjang...]"

        # Sanitize fields to prevent template injection via stray braces
        def safe(val: str) -> str:
            return str(val).replace("{", "{{").replace("}", "}}")

        # Default categories list — override via config if needed
        default_categories = (
            "PHK, Upah, Pengangguran, Lowongan_Kerja, Investasi_Tenaga_Kerja, "
            "Pelatihan_Keterampilan, Regulasi_Ketenagakerjaan, Migrasi_Pekerja, "
            "Sektor_Informal, Hubungan_Industrial, Lainnya"
        )
        from datetime import date as _date

        try:
            return self.prompt_template.format(
                title=article.get("title", "N/A"),
                source=article.get("source", "N/A"),
                published_date=article.get("published_date", "N/A"),
                url=article.get("url", "N/A"),
                content=content,
                analysis_date=_date.today().isoformat(),
                categories=getattr(self, "categories", default_categories),
            )
        except (KeyError, IndexError, ValueError) as e:
            logger.warning(f"Prompt template format error: {e}, falling back to safe build")
            return (
                f"Analisis artikel berikut:\n"
                f"Judul: {safe(article.get('title', 'N/A'))}\n"
                f"Sumber: {safe(article.get('source', 'N/A'))}\n"
                f"Konten:\n{content}\n\n"
                f"Hasilkan analisis dalam format JSON."
            )

    # --- Ollama API call (FIX: granular timeout + jitter backoff) ---
    async def _call_ollama(self, prompt: str) -> Optional[str]:
        """Send request to Ollama API with jitter backoff."""
        await self._ensure_session()

        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": self.temperature,
                "num_predict": self.max_tokens,
            },
        }

        url = f"{self.base_url}/api/generate"

        for attempt in range(1, self.max_retries + 1):
            try:
                async with self._session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        return data.get("response", "")
                    else:
                        body = await resp.text()
                        logger.warning(
                            f"Ollama returned {resp.status} (attempt {attempt}): "
                            f"{body[:200]}"
                        )
            except asyncio.TimeoutError:
                logger.warning(
                    f"Ollama timeout (attempt {attempt}/{self.max_retries})"
                )
            except Exception as e:
                logger.warning(f"Ollama error (attempt {attempt}): {e}")

            if attempt < self.max_retries:
                # Exponential backoff with jitter to avoid thundering herd
                base_delay = 2 ** attempt
                jitter = random.uniform(0, base_delay * 0.5)
                await asyncio.sleep(base_delay + jitter)

        return None

    # --- Response parsing (FIX: ReDoS-safe) ---
    @staticmethod
    def _find_balanced_json(text: str) -> Optional[str]:
        """Find the first balanced { ... } block — ReDoS-safe, no greedy regex."""
        start = text.find("{")
        if start == -1:
            return None
        depth = 0
        in_string = False
        escape_next = False
        for i in range(start, len(text)):
            ch = text[i]
            if escape_next:
                escape_next = False
                continue
            if ch == "\\":
                if in_string:
                    escape_next = True
                continue
            if ch == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start : i + 1]
        return None

    def _parse_response(self, raw: str) -> Optional[dict]:
        """Extract and parse JSON from LLM response — ReDoS-safe."""
        if not raw:
            return None

        stripped = raw.strip()

        # 1) Try direct parse
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass

        # 2) Try extracting from markdown fences (simple, non-greedy find)
        fence_start = stripped.find("```")
        if fence_start != -1:
            # Skip the opening fence line
            block_start = stripped.find("\n", fence_start)
            if block_start != -1:
                fence_end = stripped.find("```", block_start)
                if fence_end != -1:
                    block = stripped[block_start:fence_end].strip()
                    try:
                        return json.loads(block)
                    except json.JSONDecodeError:
                        pass

        # 3) Balanced brace extraction (replaces vulnerable greedy regex)
        json_str = self._find_balanced_json(stripped)
        if json_str:
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass

        self._stats["parse_errors"] += 1
        logger.error(f"Failed to parse LLM response: {stripped[:300]}...")
        return None

    # --- Core interrogation ---
    async def interrogate(self, article: dict) -> dict:
        """Analyze a single article via LLM."""
        self._stats["total_requests"] += 1
        start = time.monotonic()

        prompt = self._build_prompt(article)
        raw = await self._call_ollama(prompt)
        elapsed = round(time.monotonic() - start, 2)

        if raw is None:
            self._stats["failed"] += 1
            self._stats["total_time"] += elapsed
            logger.warning(f"No response for: {article.get('title', 'N/A')}")
            return self._empty_analysis(article, "LLM tidak merespon setelah retry")

        analysis = self._parse_response(raw)
        if analysis is None:
            self._stats["failed"] += 1
            self._stats["total_time"] += elapsed
            return self._empty_analysis(article, "Gagal parsing respons LLM")

        # Attach metadata
        analysis["_meta"] = {
            "model": self.model,
            "elapsed_seconds": elapsed,
            "url": article.get("url"),
        }

        self._stats["successful"] += 1
        self._stats["total_time"] += elapsed
        logger.info(
            f"Analyzed '{article.get('title', 'N/A')[:50]}' in {elapsed}s"
        )
        return analysis

    async def interrogate_batch(self, articles: list[dict]) -> list[dict]:
        """Analyze articles in controlled batches with semaphore."""
        sem = asyncio.Semaphore(self.batch_size)
        results: list[dict] = []

        async def _limited(art):
            async with sem:
                return await self.interrogate(art)

        try:
            tasks = [asyncio.create_task(_limited(a)) for a in articles]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Batch interrogation error: {e}")

        # Convert exceptions to empty analyses
        final = []
        for i, r in enumerate(results):
            if isinstance(r, BaseException):
                logger.error(f"Task {i} exception: {r}")
                final.append(
                    self._empty_analysis(articles[i], f"Exception: {r}")
                )
            else:
                final.append(r)

        logger.info(
            f"Batch complete: {len(final)} articles, "
            f"stats={self.stats}"
        )
        return final

    # --- Helpers ---
    @staticmethod
    def _empty_analysis(article: dict, reason: str) -> dict:
        """Return a structured empty analysis when AI fails."""
        return {
            "ringkasan": f"Analisis gagal: {reason}",
            "relevansi_ketenagakerjaan": {"skor": 0, "alasan": reason},
            "kategori": ["Lainnya"],
            "entitas": {
                "organisasi": [],
                "lokasi": [],
                "tokoh": [],
                "angka_statistik": [],
            },
            "dampak_ketenagakerjaan": {
                "langsung": "N/A",
                "tidak_langsung": "N/A",
                "skala": "N/A",
                "sentimen": "netral",
            },
            "indikator_bps": {
                "terkait_sakernas": False,
                "indikator": [],
                "potensi_data": "N/A",
            },
            "timeline": {
                "peristiwa_utama": "N/A",
                "dampak_estimasi": "N/A",
            },
            "metadata_analisis": {
                "confidence": 0.0,
                "limitasi": reason,
                "saran_tindak_lanjut": "Coba ulang analisis atau periksa koneksi Ollama",
            },
            "_meta": {
                "model": "N/A",
                "elapsed_seconds": 0,
                "url": article.get("url"),
                "error": reason,
            },
        }

    @property
    def stats(self) -> dict:
        total = self._stats["total_requests"]
        avg = self._stats["total_time"] / total if total > 0 else 0
        return {
            **self._stats,
            "avg_time_per_request": round(avg, 2),
            "success_rate": (
                f"{self._stats['successful']}/{total}" if total > 0 else "0/0"
            ),
        }