"""Daily ingestion prototype for Boletín Oficial CABA -> Licitaciones JSONL.

    python ingest.py --days 7 --out out/            # last 7 days (ending yesterday)
    python ingest.py --from 2026-11-11 --to 2026-11-18

Design:
  * asyncio + httpx, one HTTP/1.1 connection pool, bounded concurrency (default 6)
    plus a minimum interval between request starts -> polite, predictable load.
  * retries with exponential backoff + jitter on timeouts / 429 / 5xx, honours Retry-After.
  * raw cache keyed by id_norma (pdf/{id}.pdf, the API day JSON in json/{date}.json);
    anything already cached is never re-downloaded -> reruns/backfills are idempotent.
    In production the cache is an object store (R2/S3) with the same keys.
  * http:// download links are rewritten to https:// (skips the BigIP 302 hop).
  * annex PDFs are fetched for award stages (the award table is often in the annex).
  * download failures are recorded as errors, never as "empty text".
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import random
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
import pypdfium2 as pdfium

from parse_lic import AMOUNT_STAGES, normalize_stage, parse_item, parse_nombre

API = "https://api-restboletinoficial.buenosaires.gob.ar"
UA = "boletin-licitaciones-monitor/0.1 (+contact via repository)"


class Polite:
    """Concurrency cap + minimum spacing between request starts."""

    def __init__(self, concurrency: int, min_interval: float):
        self.sem = asyncio.Semaphore(concurrency)
        self.min_interval = min_interval
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def __aenter__(self):
        await self.sem.acquire()
        async with self._lock:
            now = time.monotonic()
            wait = self._next - now
            self._next = max(now, self._next) + self.min_interval
        if wait > 0:
            await asyncio.sleep(wait)

    async def __aexit__(self, *exc):
        self.sem.release()


class Fetcher:
    def __init__(self, client: httpx.AsyncClient, polite: Polite, retries: int = 4):
        self.c, self.p, self.retries = client, polite, retries
        self.stats = {"requests": 0, "retries": 0, "bytes": 0, "latencies": []}

    async def get(self, url: str, **kw) -> httpx.Response:
        last_exc = None
        for attempt in range(self.retries + 1):
            async with self.p:
                t0 = time.monotonic()
                try:
                    self.stats["requests"] += 1
                    r = await self.c.get(url, **kw)
                    self.stats["latencies"].append(time.monotonic() - t0)
                except (httpx.TimeoutException, httpx.TransportError) as e:
                    last_exc, r = e, None
            if r is not None:
                if r.status_code < 400 or r.status_code == 404:
                    self.stats["bytes"] += len(r.content)
                    return r
                if r.status_code not in (408, 425, 429, 500, 502, 503, 504):
                    r.raise_for_status()
                last_exc = httpx.HTTPStatusError(f"HTTP {r.status_code}", request=r.request, response=r)
            if attempt == self.retries:
                break
            self.stats["retries"] += 1
            ra = r.headers.get("retry-after") if r is not None else None
            delay = float(ra) if ra and ra.isdigit() else min(30, 2 ** attempt) * (0.5 + random.random())
            await asyncio.sleep(delay)
        raise last_exc  # type: ignore[misc]


def https(url: str) -> str:
    return "https://" + url[len("http://"):] if url.startswith("http://") else url


def pdf_text(data: bytes) -> tuple[str, int]:
    doc = pdfium.PdfDocument(data)
    try:
        pages = [doc[i].get_textpage().get_text_range() for i in range(len(doc))]
    finally:
        doc.close()
    return "\n".join(pages).replace("\r\n", "\n").replace("\r", "\n"), len(pages)


def flatten(day_json: dict, fecha: str) -> list[dict]:
    rows = []
    normas = (day_json.get("normas") or {}).get("normas") or {}
    for sub, tipos in normas.items():
        for tipo, orgs in (tipos or {}).items():
            for org, lst in (orgs or {}).items():
                for n in lst or []:
                    rows.append({"fecha": fecha, "subseccion": sub, "tipo_norma": tipo, "organismo": org, **n})
    return rows


async def fetch_boletin(f: Fetcher, param: str, cache: Path, key: str | None = None) -> tuple[dict | None, str | None]:
    """param: 'dd-mm-yyyy' or a boletín number. Cached by boletín number (immutable once published).
    NB: for a date without a boletín (weekends) the API returns the LATEST boletín, not a 404, and a date
    returns only one boletín even when two were published that day (e.g. 7494 and 7495 on 17/11/2026)."""
    if key:
        p = cache / "json" / f"{key}.json"
        if p.exists():
            return json.loads(p.read_text()), None
    try:
        r = await f.get(f"{API}/obtenerBoletin/{param}/true")
    except Exception as e:  # noqa: BLE001
        return None, f"day_fetch_failed: {e!r}"
    if r.status_code == 404:
        return None, "no_boletin"
    try:
        j = r.json()
    except ValueError:
        return None, "bad_json"
    if not isinstance(j, dict) or "boletin" not in j:
        return None, "no_boletin"
    if isinstance(j.get("normas"), dict) and j["normas"].get("errores"):
        return None, f"server_error: {j['normas']['errores'][:1]}"  # do NOT cache: retry next run
    p = cache / "json" / f"{j['boletin']['numero']}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(j, ensure_ascii=False))
    return j, None


async def fetch_pdf(f: Fetcher, url: str, path: Path, manifest: dict) -> tuple[bytes | None, str | None]:
    if path.exists() and path.stat().st_size > 0:
        return path.read_bytes(), None
    try:
        r = await f.get(https(url))
    except Exception as e:  # noqa: BLE001
        return None, f"download_failed: {e!r}"
    if r.status_code == 404:
        return None, "pdf_404"
    data = r.content
    if not data.startswith(b"%PDF"):
        return None, f"not_pdf ({r.headers.get('content-type')}, {len(data)}B)"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".part")
    tmp.write_bytes(data)
    tmp.replace(path)  # atomic: never leaves half files that look cached
    manifest[path.name] = {
        "url": url, "final_url": str(r.url), "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data),
        "etag": r.headers.get("etag"), "last_modified": r.headers.get("last-modified"),
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
    }
    return data, None


async def process_item(f: Fetcher, rec: dict, cache: Path, manifest: dict) -> dict:
    t0 = time.monotonic()
    out = {"id_norma": rec["id_norma"], "fecha": rec["fecha"], "boletin_numero": rec.get("boletin_numero"), "nombre": rec.get("nombre"),
           "sumario": rec.get("sumario"), "organismo": rec.get("organismo"), "url_norma": rec.get("url_norma"),
           "errors": []}
    data, err = await fetch_pdf(f, rec["url_norma"], cache / "pdf" / f"{rec['id_norma']}.pdf", manifest)
    text, pages = "", 0
    if err:
        out["errors"].append(err)
    else:
        try:
            text, pages = pdf_text(data)
        except Exception as e:  # noqa: BLE001
            out["errors"].append(f"pdf_parse_failed: {e!r}")
    # annexes: only for stages that carry amounts (keeps request volume down)
    stage = normalize_stage(parse_nombre(rec.get("nombre"))["etapa_raw"])
    annex_texts = []
    if stage in AMOUNT_STAGES:
        for k, ax in enumerate(rec.get("anexos") or []):
            u = ax.get("filenet_firmado")
            if not u:
                continue
            adata, aerr = await fetch_pdf(f, u, cache / "anexos" / f"{rec['id_norma']}_{k}.pdf", manifest)
            if aerr:
                out["errors"].append(f"anexo{k}: {aerr}")
                continue
            try:
                annex_texts.append(pdf_text(adata)[0])
            except Exception as e:  # noqa: BLE001
                out["errors"].append(f"anexo{k} parse: {e!r}")
    full = text + ("\n\n" + "\n\n".join(annex_texts) if annex_texts else "")
    out["pages"] = pages
    out["n_anexos"] = len(rec.get("anexos") or [])
    out["text_chars"] = len(full)
    out["text_sha256"] = hashlib.sha256(full.encode()).hexdigest() if full else None
    out["text"] = full
    parsed = parse_item(rec, full) if not out["errors"] or full else None
    out["parsed"] = parsed
    out["elapsed_s"] = round(time.monotonic() - t0, 3)
    return out


async def run(start: date, end: date, out_dir: Path, concurrency: int, min_interval: float) -> dict:
    cache = out_dir / "cache"
    cache.mkdir(parents=True, exist_ok=True)
    mpath = cache / "manifest.json"
    manifest = json.loads(mpath.read_text()) if mpath.exists() else {}
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    timeout = httpx.Timeout(connect=10, read=30, write=10, pool=60)
    t0 = time.monotonic()
    async with httpx.AsyncClient(headers={"User-Agent": UA}, follow_redirects=True, limits=limits,
                                 timeout=timeout, http2=False) as client:
        f = Fetcher(client, Polite(concurrency, min_interval))
        days = [start + timedelta(n) for n in range((end - start).days + 1)]
        # 1) dates only give us the boletín-number bounds; 2) crawl every number in between
        #    (catches second editions that a date query never returns)
        day_res = await asyncio.gather(*(fetch_boletin(f, f"{d:%d-%m-%Y}", cache) for d in days))
        day_errors, bols = [], {}
        for d, (j, err) in zip(days, day_res):
            if err:
                day_errors.append({"param": d.isoformat(), "error": err})
                continue
            fp = datetime.strptime(j["boletin"]["fecha_publicacion"], "%d/%m/%Y").date()
            if start <= fp <= end:
                bols[j["boletin"]["numero"]] = j
        if bols:
            gap = [n for n in range(min(bols), max(bols) + 1) if n not in bols]
            gap_res = await asyncio.gather(*(fetch_boletin(f, str(n), cache, key=str(n)) for n in gap))
            for n, (j, err) in zip(gap, gap_res):
                if err:
                    day_errors.append({"param": n, "error": err})
                else:
                    bols[n] = j
        t_days = time.monotonic() - t0
        items = []
        for n, j in sorted(bols.items()):
            fp = datetime.strptime(j["boletin"]["fecha_publicacion"], "%d/%m/%Y").date().isoformat()
            items += [{**r, "boletin_numero": n} for r in flatten(j, fp) if r["subseccion"] == "Licitaciones"]
        # idempotency: one row per id_norma even if a norm is republished
        uniq = {}
        for r in items:
            uniq.setdefault(r["id_norma"], r)
        results = await asyncio.gather(*(process_item(f, r, cache, manifest) for r in uniq.values()))
    wall = time.monotonic() - t0
    mpath.write_text(json.dumps(manifest, ensure_ascii=False, indent=0))

    with open(out_dir / "licitaciones.jsonl", "w") as fh:
        for r in sorted(results, key=lambda x: (x["fecha"], x["id_norma"])):
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    lat = sorted(f.stats["latencies"]) or [0]
    parsed = [r["parsed"] for r in results if r["parsed"]]
    amt_stage = [p for p in parsed if p["etapa"] in AMOUNT_STAGES]
    stats = {
        "range": [start.isoformat(), end.isoformat()], "concurrency": concurrency, "min_interval_s": min_interval,
        "wall_s": round(wall, 1), "days_s": round(t_days, 1), "boletines": sorted(bols),
        "day_errors": day_errors, "items": len(results),
        "item_failures": sum(bool(r["errors"]) for r in results),
        "failure_detail": [{"id_norma": r["id_norma"], "errors": r["errors"]} for r in results if r["errors"]],
        "http_requests": f.stats["requests"], "http_retries": f.stats["retries"], "mb": round(f.stats["bytes"] / 1e6, 2),
        "latency_p50_s": round(lat[len(lat) // 2], 2), "latency_p95_s": round(lat[int(len(lat) * .95)], 2),
        "latency_max_s": round(lat[-1], 2),
        "amount_stage_items": len(amt_stage),
        "amount_hit": sum(bool(p["totals"]) for p in amt_stage),
        "with_cuit": sum(bool(p["winners"]) for p in amt_stage),
        "high_confidence": sum(p["confidence"] == "high" for p in amt_stage),
        "needs_llm": sum(p["needs_llm"] for p in amt_stage),
        "usd_items": sum("USD" in p["totals"] for p in amt_stage),
        "with_bac_code": sum(bool(p["codigo_bac"]) for p in parsed),
        "stages": {},
    }
    for p in parsed:
        stats["stages"][p["etapa"]] = stats["stages"].get(p["etapa"], 0) + 1
    (out_dir / "run_stats.json").write_text(json.dumps(stats, ensure_ascii=False, indent=1))
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=1)
    ap.add_argument("--from", dest="dfrom")
    ap.add_argument("--to", dest="dto")
    ap.add_argument("--out", default="out")
    ap.add_argument("--concurrency", type=int, default=6)
    ap.add_argument("--min-interval", type=float, default=0.15, help="seconds between request starts")
    a = ap.parse_args()
    if a.dfrom:
        start = date.fromisoformat(a.dfrom)
        end = date.fromisoformat(a.dto) if a.dto else date.today()
    else:
        end = date.today()
        start = end - timedelta(days=a.days)
    assert a.concurrency <= 8, "keep it polite"
    stats = asyncio.run(run(start, end, Path(a.out), a.concurrency, a.min_interval))
    print(json.dumps({k: v for k, v in stats.items() if k != "failure_detail"}, indent=1, ensure_ascii=False))


if __name__ == "__main__":
    main()
