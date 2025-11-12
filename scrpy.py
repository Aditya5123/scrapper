import os
import time
import math
import requests
from typing import Dict, Any, List, Optional

# ================== CONFIG ==================
API_URL = "https://api.messefrankfurt.com/service/esb_api/exhibitor-service/api/2.1/public/exhibitor/search"
API_KEY = os.getenv("MF_API_KEY", "LXnMWcYQhipLAS7rImEzmZ3CkrU033FMha9cwVSngG4vbufTsAOCQQ==")  # <-- put your key here
EVENT_ID = "HEIMTEXTIL"  # from your request
LANG = "en-GB"
ORDER_BY = "name"
PAGE_SIZE = 30                 # you can raise to 100 if the API allows
START_PAGE = 1                 # start page number (inclusive)
MAX_PAGES: Optional[int] = None  # set to an int to hard-cap pages, or None to auto-stop
OR_SEARCH_FALLBACK = "false"
SHOW_JUMP_LABELS = "true"
QUERY = ""                     # q= (blank → all)
OUT_CSV = "HEIMTEXTIL__exhibitors.csv"
OUT_XLSX = "HEIMTEXTIL__exhibitors.xlsx"

# polite retry/backoff
RETRY_STATUS = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
BACKOFF = 1.5  # seconds
TIMEOUT = 30   # seconds
# ============================================


def build_headers() -> Dict[str, str]:
    return {
        "accept": "application/json, text/plain, */*",
        "apikey": "LXnMWcYQhipLAS7rImEzmZ3CkrU033FMha9cwVSngG4vbufTsAOCQQ==",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "origin": "https://heimtextil.messefrankfurt.com/frankfurt/en/exhibitor-search.html?page=1&pagesize=90",
        "referer": "hhttps://heimtextil.messefrankfurt.com/frankfurt/en/exhibitor-search.html?page=1&pagesize=90",
    }


def fetch_page(session: requests.Session, page_number: int) -> Dict[str, Any]:
    params = {
        "language": LANG,
        "q": QUERY,
        "orderBy": ORDER_BY,
        "pageNumber": page_number,
        "pageSize": PAGE_SIZE,
        "orSearchFallback": OR_SEARCH_FALLBACK,
        "showJumpLabels": SHOW_JUMP_LABELS,
        "findEventVariable": EVENT_ID,
    }

    retries = 0
    while True:
        resp = session.get(API_URL, headers=build_headers(), params=params, timeout=TIMEOUT)
        if resp.status_code in RETRY_STATUS and retries < MAX_RETRIES:
            wait = BACKOFF * (retries + 1)
            time.sleep(wait)
            retries += 1
            continue
        resp.raise_for_status()
        return resp.json()


def first_or_blank(seq: Optional[List[Any]], key: Optional[str] = None) -> str:
    """
    Utility: returns the first element or blank. If `key` is provided,
    extracts that key from the first dict in the list.
    """
    if not seq:
        return ""
    first = seq[0]
    if key is None:
        return str(first) if first is not None else ""
    if isinstance(first, dict):
        return str(first.get(key, "") or "")
    return ""


def flatten_hit(hit: Dict[str, Any]) -> Dict[str, Any]:
    ex = hit.get("exhibitor", {}) or {}

    # address block
    addr = ex.get("address", {}) or {}
    addr_rdm = ex.get("addressrdm", {}) or {}

    # hall/booth from exhibition.exhibitionHall[*].stand[*]
    hall = ""
    booth = ""
    exhibition = ex.get("exhibition") or {}
    halls = exhibition.get("exhibitionHall") or []
    if halls:
        # pick first hall
        hall = str(halls[0].get("name") or halls[0].get("id") or "")
        stands = halls[0].get("stand") or []
        booth = first_or_blank(stands, "name")

    # keywords
    keywords = ex.get("keyWords") or []
    keywords_str = ", ".join([str(k).strip() for k in keywords if k])

    # products count (optional)
    products = ex.get("products") or {}
    products_count = products.get("countTotal", 0)

    # presentation/stands (some entries keep booth here too)
    p_links = ex.get("presentationLinks") or []
    first_p = p_links[0] if p_links else {}
    # if booth empty above, try from presentationLinks
    if not booth:
        stands2 = first_p.get("pstands") or []
        booth = first_or_blank(stands2, "firstBoothNumber")

    # compose flat record
    row = {
        "exhibitor_id": ex.get("id", ""),
        "name": ex.get("name", ""),
        "rewriteId": ex.get("rewriteId", ""),
        "country_iso3": (addr.get("country", {}) or {}).get("iso3", ""),
        "country": (addr.get("country", {}) or {}).get("label", ""),
        "city": addr.get("city", ""),
        "zip": addr.get("zip", ""),
        "street": addr.get("street", ""),
        "phone": addr.get("tel", ""),
        "fax": addr.get("fax", ""),
        "email": addr.get("email", ""),
        "homepage": ex.get("homepage", "") or ex.get("href", ""),
        "hall": hall,
        "booth": booth,
        "logo": ex.get("logo", ""),
        "keywords": keywords_str,
        "products_count": products_count,
        "exhibition_id": exhibition.get("id", ""),
        "exhibition_name": exhibition.get("name", ""),
        "exhibition_start": exhibition.get("startdate", ""),
        "exhibition_end": exhibition.get("enddate", ""),
        "last_approval_date": ex.get("lastApprovalDate", ""),
        "sortKey": ex.get("sortKey", ""),
    }

    # optional: formatted postal address if present
    row["postal_formatted"] = addr_rdm.get("formatedAddress", "")

    return row


def main():
    session = requests.Session()
    all_rows: List[Dict[str, Any]] = []
    page = START_PAGE
    seen_empty_pages = 0

    print("📥 Collecting exhibitors…")
    while True:
        if MAX_PAGES is not None and page > MAX_PAGES:
            break

        data = fetch_page(session, page)

        # response usually shaped like: {"success": true, "result": {"hits": [...], ...}}
        result = (data or {}).get("result") or {}
        hits = result.get("hits") or []

        if not hits:
            seen_empty_pages += 1
            # stop if first empty page or after a couple of empties
            if seen_empty_pages >= 1:
                print(f"✅ Done. Stopped at page {page} (no more results).")
                break
        else:
            seen_empty_pages = 0

        # flatten each hit
        for h in hits:
            all_rows.append(flatten_hit(h))

        print(f"  • Page {page}: +{len(hits)} rows (total={len(all_rows)})")
        page += 1
        # be gentle
        time.sleep(0.3)

    if not all_rows:
        print("No data collected. Check API key/event ID/params.")
        return

    # ---- Save CSV ----
    import csv
    fieldnames = list(all_rows[0].keys())
    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"💾 Saved CSV: {OUT_CSV}")

    # ---- Save Excel ----
    try:
        import pandas as pd
        df = pd.DataFrame(all_rows)
        # order columns (optional)
        preferred = [
            "name","exhibitor_id","rewriteId","country","country_iso3","city","zip","street",
            "phone","fax","email","homepage","hall","booth","logo","keywords","products_count",
            "exhibition_id","exhibition_name","exhibition_start","exhibition_end",
            "last_approval_date","sortKey","postal_formatted"
        ]
        cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
        df = df[cols]
        df.to_excel(OUT_XLSX, index=False)
        print(f"💾 Saved Excel: {OUT_XLSX}")
    except ImportError:
        print("Pandas not installed; skipped Excel export. Install with: pip install pandas openpyxl")

    print("✅ Finished.")


if __name__ == "__main__":
    main()
