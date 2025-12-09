#!/usr/bin/env python3
import argparse
import asyncio
import dataclasses
import json
import csv
import os
import re
import sys
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Dict, Any

import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import urllib.robotparser as robotparser


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


@dataclasses.dataclass
class Restaurant:
    uuid: str
    slug: str
    name: str
    website: Optional[str]


@dataclasses.dataclass
class MenuPage:
    url: str
    label: str
    score: int
    content_type: Optional[str] = None
    status_code: Optional[int] = None
    is_menu_like: Optional[bool] = None
    guessed: bool = False  # track if this URL is a guessed endpoint


PRICE_RE = re.compile(r"\b(?:\$\s*)?\d{1,3}(?:[\.,]\d{2})?\b")
CURRENCY_RE = re.compile(r"[$£€]")
CATEGORY_KEYWORDS = {
    "appetizer", "appetizers", "starters", "sides", "entrees", "mains", "main", "pasta",
    "pizzas", "pizza", "desserts", "brunch", "lunch", "dinner", "drinks", "beverages",
    "beverage", "wine", "cocktails", "beer", "kids", "salads", "soup", "soups",
}
POSITIVE_MENU_HINTS = {
    "menu", "menus", "our menu", "food", "order", "order online",
    "lunch", "dinner", "brunch", "drinks", "beverages", "happy hour"
}
NEGATIVE_HINTS = {
    "reservations", "reservation", "gift", "careers", "jobs", "press",
    "gallery", "photos", "contact", "privacy", "terms", "accessibility"
}

EXTERNAL_MENU_HOSTS = {
    "toasttab.com", "clover.com", "square.site", "ubereats.com", "doordash.com",
    "grubhub.com", "opentable.com", "resy.com", "tock.com", "olo.com",
}

COMMON_MENU_PATHS = [
    "/menu",
    "/menus",
    "/our-menu",
    "/our-menus",
    "/food-menu",
    "/food-menus",
    "/dinner-menu",
    "/lunch-menu",
    "/brunch-menu",
    "/breakfast-menu",
    "/kids-menu",
    "/kid-menu",
    "/kids",
    "/drinks-menu",
    "/drink-menu",
    "/beverages-menu",
    "/beverage-menu",
    "/wine-menu",
    "/dessert-menu",
    "/specials",
    "/daily-specials",
    "/today-specials",
    "/buffet-menu",
    "/takeaway-menu",
    "/take-out-menu",
    "/delivery-menu",
    "/order-online/menu",
    "/order/menu",

    # HTML file variants
    "/menu.html",
    "/menus.html",
    "/our-menu.html",
    "/food-menu.html",
    "/lunch-menu.html",
    "/dinner-menu.html",

    # PDF variants
    "/menu.pdf",
    "/menus.pdf",
    "/our-menu.pdf",
    "/food-menu.pdf",
    "/dinner-menu.pdf",
    "/lunch-menu.pdf",
    "/brunch-menu.pdf",
    "/breakfast-menu.pdf",
    "/kids-menu.pdf",
    "/drinks-menu.pdf",
    "/drink-menu.pdf",
    "/takeaway-menu.pdf",
    "/take-out-menu.pdf",
    "/dessert-menu.pdf",
    "/wine-menu.pdf",
    "/specials.pdf",
    "/buffet-menu.pdf",

    # Folder-based patterns often used in WordPress / restaurant sites
    "/wp-content/uploads/menu.pdf",
    "/wp-content/uploads/menus.pdf",
    "/wp-content/uploads/2023/menu.pdf",
    "/wp-content/uploads/2024/menu.pdf",
    "/wp-content/uploads/2022/menu.pdf",
    "/files/menu.pdf",
    "/uploads/menu.pdf",
    "/download/menu.pdf",
    "/documents/menu.pdf",
    "/docs/menu.pdf",

    # Multi-category menu pages
    "/menu/dinner",
    "/menu/lunch",
    "/menu/brunch",
    "/menu/breakfast",
    "/menu/drinks",
    "/menu/beverages",
    "/menu/kids",
    "/menu/dessert",
    "/menu/specials",

    # Common restaurant CMS patterns
    "/restaurant-menu",
    "/the-menu",
    "/menus-list",
    "/full-menu",
    "/complete-menu",
    "/menu-card",
    "/menu-list",
    "/food",
    "/eat",
    "/order",
    "/order-online",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_restaurants(path: str) -> List[Restaurant]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    restaurants: List[Restaurant] = []
    for r in data:
        name = (r.get("name") or "restaurant").strip()
        slug_source = name.lower()
        slug = re.sub(r"[^a-z0-9]+", "-", slug_source).strip("-") or "restaurant"
        restaurants.append(Restaurant(
            uuid=r.get("uuid") or "",
            slug=slug,
            name=name,
            website=(r.get("website") or "").strip() or None,
        ))
    return restaurants


def normalize_url(url: str) -> str:
    if not url:
        return url
    parsed = urlparse(url)
    if not parsed.scheme:
        return f"https://{url}"
    return url


def host_matches_external(host: str) -> bool:
    host = (host or "").lower()
    for dom in EXTERNAL_MENU_HOSTS:
        if host == dom or host.endswith("." + dom):
            return True
    return False


def same_host(url1: str, url2: str) -> bool:
    return urlparse(url1).hostname == urlparse(url2).hostname


def build_robots_cache() -> Dict[str, robotparser.RobotFileParser]:
    return {}


async def fetch_robots(
    client: httpx.AsyncClient,
    base_url: str,
    cache: Dict[str, robotparser.RobotFileParser],
) -> robotparser.RobotFileParser:
    host = urlparse(base_url).hostname or ""
    if host in cache:
        return cache[host]
    robots_url = f"{urlparse(base_url).scheme}://{host}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        r = await client.get(robots_url, timeout=10)
        if r.status_code == 200:
            rp.parse(r.text.splitlines())
        else:
            rp.parse("")
    except Exception:
        rp.parse("")
    cache[host] = rp
    return rp


def score_link(text: str, href: str) -> int:
    t = (text or "").strip().lower()
    h = (href or "").strip().lower()
    score = 0

    # Text-based hints
    for kw in POSITIVE_MENU_HINTS:
        if kw in t or kw in h:
            score += 3 if "menu" in kw else 2
    for bad in NEGATIVE_HINTS:
        if bad in t or bad in h:
            score -= 3

    # Strong URL-based hints
    if any(p in h for p in ["/menu", "/menus", "food-menu", "our-menu", "menu.html"]):
        score += 3

    # PDF is very likely a menu
    if h.endswith(".pdf"):
        score += 4

    return score


def discover_menu_links(base_url: str, html: str) -> List[MenuPage]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: Dict[str, MenuPage] = {}
    for a in soup.find_all("a"):
        href = a.get("href")
        if not href:
            continue

        abs_url = urljoin(base_url, href)
        text = a.get_text(" ", strip=True)[:200]

        s = score_link(text, href)

        # Extra boost for known external menu/ordering hosts
        host = urlparse(abs_url).hostname or ""
        if host_matches_external(host):
            s += 6

        if s <= 0:
            continue

        mp = MenuPage(url=abs_url, label=(text or "menu").lower()[:60], score=s)
        if abs_url not in candidates or candidates[abs_url].score < s:
            candidates[abs_url] = mp

    # Consider the homepage itself if it looks menu-like
    if is_menu_like(html):
        candidates[base_url] = MenuPage(url=base_url, label="menu", score=5)

    return sorted(candidates.values(), key=lambda m: m.score, reverse=True)[:12]


def build_guessed_menu_pages(base_url: str) -> List[MenuPage]:
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    pages: List[MenuPage] = []
    for path in COMMON_MENU_PATHS:
        url = urljoin(root + "/", path.lstrip("/"))
        label = path.strip("/").replace("-", " ") or "menu"
        pages.append(MenuPage(url=url, label=label[:60], score=8, guessed=True))
    return pages


def is_menu_like(html: str) -> bool:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    price_hits = len(PRICE_RE.findall(text))
    currency_hits = len(CURRENCY_RE.findall(text))
    text_lower = text.lower()
    cat_hits = sum(1 for kw in CATEGORY_KEYWORDS if kw in text_lower)

    if price_hits >= 3:
        return True
    if currency_hits >= 2:
        return True
    if price_hits >= 1 and cat_hits >= 3:
        return True
    return False


def looks_like_error_page(html: str, status_code: Optional[int]) -> bool:
    """Detect typical 404/error pages."""
    if status_code is not None and status_code >= 400:
        return True

    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.string or "").strip().lower() if soup.title and soup.title.string else ""

    if any(x in title for x in ["404", "page not found", "not found", "error"]):
        return True

    meta_robots = soup.find("meta", attrs={"name": "robots"})
    robots_content = (meta_robots.get("content") or "").lower() if meta_robots else ""
    text = soup.get_text(" ", strip=True).lower()

    if "noindex" in robots_content and any(
        phrase in text for phrase in [
            "page does not exist", "page not found", "this page does not exist", "oops"
        ]
    ):
        return True

    return False


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


async def fetch_text(client: httpx.AsyncClient, url: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    try:
        r = await client.get(url, timeout=20)
        ctype = r.headers.get("Content-Type", "").split(";")[0].strip().lower()
        if "text/html" in ctype or ctype == "application/xhtml+xml" or ctype == "":
            return r.text, ctype or "text/html", r.status_code
        return None, ctype, r.status_code
    except Exception:
        return None, None, None


def sanitize_label(label: str) -> str:
    label = label.lower().strip() or "menu"
    label = re.sub(r"[^a-z0-9]+", "-", label).strip("-")
    if not label:
        label = "menu"
    return label[:60]


def restaurant_output_dir(out_root: str, r: Restaurant) -> Tuple[str, str]:
    slug_unique = f"{r.slug}--{(r.uuid or '')[:8]}" if r.uuid else r.slug
    out_dir = os.path.join(out_root, slug_unique)
    return out_dir, slug_unique


def existing_menu_info(out_root: str, r: Restaurant) -> Optional[Dict[str, Any]]:
    """
    If this restaurant already has menu files in its folder under out_root,
    return a result dict populated from metadata.json (if present) or from
    directory contents. Otherwise return None.
    """
    out_dir, slug_unique = restaurant_output_dir(out_root, r)
    if not os.path.isdir(out_dir):
        return None

    meta_path = os.path.join(out_dir, "metadata.json")
    if os.path.isfile(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            saved_files = meta.get("saved_files") or []
            # If metadata says we have saved_files, treat as existing menu
            if saved_files:
                meta_result = {
                    "uuid": meta.get("uuid", r.uuid),
                    "slug": slug_unique,
                    "name": meta.get("name", r.name),
                    "website": meta.get("website", r.website),
                    "found": True,
                    "saved_files": saved_files,
                    "errors": meta.get("errors", []),
                    "skipped": True,
                }
                meta_result["errors"].append("skipped_existing_menu")
                return meta_result
        except Exception:
            # If metadata is broken, fall through to directory scan
            pass

    # If no useful metadata or no saved_files, scan directory for html/pdf
    saved_files: List[Dict[str, Any]] = []
    try:
        for fn in os.listdir(out_dir):
            if fn == "metadata.json":
                continue
            full_path = os.path.join(out_dir, fn)
            if not os.path.isfile(full_path):
                continue
            ext = os.path.splitext(fn)[1].lower()
            if ext in (".html", ".htm", ".pdf"):
                saved_files.append({
                    "url": None,
                    "file": os.path.relpath(full_path),
                    "status": None,
                    "content_type": None,
                    "is_menu_like": True,
                })
    except FileNotFoundError:
        return None

    if saved_files:
        return {
            "uuid": r.uuid,
            "slug": slug_unique,
            "name": r.name,
            "website": r.website,
            "found": True,
            "saved_files": saved_files,
            "errors": ["skipped_existing_menu_no_meta"],
            "skipped": True,
        }

    # Folder exists but only metadata.json or non-menu files -> need to scrape
    return None


async def process_restaurant(
    client: httpx.AsyncClient,
    rp_cache: Dict[str, robotparser.RobotFileParser],
    out_root: str,
    r: Restaurant,
    max_pages_per_site: int = 5,
) -> Dict[str, Any]:
    # First: check if we already have menus for this restaurant
    existing = existing_menu_info(out_root, r)
    if existing is not None:
        # We have existing menu files -> skip scraping
        return existing

    result: Dict[str, Any] = {
        "uuid": r.uuid,
        "slug": r.slug,
        "name": r.name,
        "website": r.website,
        "found": False,
        "saved_files": [],
        "errors": [],
    }

    if not r.website:
        result["errors"].append("no_website")
        return result

    base_url = normalize_url(r.website)
    out_dir, slug_unique = restaurant_output_dir(out_root, r)
    ensure_dir(out_dir)

    rp = await fetch_robots(client, base_url, rp_cache)
    if not rp.can_fetch(USER_AGENT, base_url):
        result["errors"].append("blocked_by_robots_home")
        meta_path = os.path.join(out_dir, "metadata.json")
        try:
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump({
                    "uuid": r.uuid,
                    "name": r.name,
                    "website": base_url,
                    "timestamp": now_iso(),
                    "found": False,
                    "saved_files": [],
                    "errors": result["errors"],
                }, f, indent=2)
        except Exception:
            pass
        return result

    html, ctype, status = await fetch_text(client, base_url)
    if html is None:
        result["errors"].append(f"homepage_unavailable:{status}:{ctype}")
        meta_path = os.path.join(out_dir, "metadata.json")
        try:
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump({
                    "uuid": r.uuid,
                    "name": r.name,
                    "website": base_url,
                    "timestamp": now_iso(),
                    "found": False,
                    "saved_files": [],
                    "errors": result["errors"],
                }, f, indent=2)
        except Exception:
            pass
        return result

    candidates = discover_menu_links(base_url, html)
    guessed_candidates = build_guessed_menu_pages(base_url)
    existing_urls = {m.url for m in candidates}
    for mp in guessed_candidates:
        if mp.url not in existing_urls:
            candidates.append(mp)

    seen_urls = set()
    saved = 0

    for mp in candidates:
        if saved >= max_pages_per_site:
            break
        if mp.url in seen_urls:
            continue
        seen_urls.add(mp.url)

        rp_target = await fetch_robots(client, mp.url, rp_cache)
        if not rp_target.can_fetch(USER_AGENT, mp.url):
            result["errors"].append(f"blocked_by_robots:{mp.url}")
            continue

        body, ctype2, status2 = await fetch_text(client, mp.url)
        mp.content_type = ctype2
        mp.status_code = status2

        # PDFs
        if ctype2 == "application/pdf" or (ctype2 and "pdf" in ctype2):
            mp.is_menu_like = True
            label = sanitize_label(mp.label or "menu")
            if label == "menu":
                path = urlparse(mp.url).path
                tail = os.path.basename(path) or "menu"
                tail = sanitize_label(tail)
                if tail and tail != "menu":
                    label = tail

            filename = f"{label}.pdf"
            save_path = os.path.join(out_dir, filename)
            try:
                pdf_resp = await client.get(mp.url, timeout=20)
                with open(save_path, "wb") as f:
                    f.write(pdf_resp.content)

                result["saved_files"].append({
                    "url": mp.url,
                    "file": os.path.relpath(save_path),
                    "status": status2,
                    "content_type": ctype2,
                    "is_menu_like": True,
                })
                saved += 1
            except Exception as e:
                result["errors"].append(f"write_failed_pdf:{mp.url}:{e}")
            continue

        # Non-PDF: need body
        if body is None:
            result["errors"].append(f"fetch_failed:{mp.url}:{status2}:{ctype2}")
            continue

        # Skip obvious error/404 pages
        if looks_like_error_page(body, status2):
            result["errors"].append(f"error_page:{mp.url}:{status2}")
            continue

        host = urlparse(mp.url).hostname or ""
        is_external_menu_host = host_matches_external(host)

        mp.is_menu_like = is_menu_like(body)

        if not mp.is_menu_like and not is_external_menu_host:
            path = urlparse(mp.url).path.lower()
            base_name = os.path.basename(path)
            ends_menu = path.endswith("/menu") or path.endswith("/menus")
            base_is_menu = (
                base_name in {"menu", "menus"} or
                base_name.startswith("menu-") or
                base_name.startswith("menus-")
            )

            # For guessed URLs, require menu-like content; don't rely only on path.
            if mp.guessed:
                continue

            if not (ends_menu or base_is_menu):
                continue
        elif is_external_menu_host:
            mp.is_menu_like = True

        label = sanitize_label(mp.label or "menu")
        if label == "menu":
            path = urlparse(mp.url).path
            tail = os.path.basename(path) or "menu"
            tail = sanitize_label(tail)
            if tail and tail != "menu":
                label = tail

        filename = f"{label}.html"
        save_path = os.path.join(out_dir, filename)
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(body)
            result["saved_files"].append({
                "url": mp.url,
                "file": os.path.relpath(save_path),
                "status": status2,
                "content_type": ctype2,
                "is_menu_like": mp.is_menu_like,
            })
            saved += 1
        except Exception as e:
            result["errors"].append(f"write_failed:{mp.url}:{e}")

    result["found"] = saved > 0

    meta_path = os.path.join(out_dir, "metadata.json")
    try:
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump({
                "uuid": r.uuid,
                "name": r.name,
                "website": base_url,
                "timestamp": now_iso(),
                "found": result["found"],
                "saved_files": result["saved_files"],
                "errors": result["errors"],
            }, f, indent=2)
    except Exception as e:
        result["errors"].append(f"meta_write_failed:{e}")

    return result


async def run(
    restaurants_json: str,
    out_root: str,
    limit: Optional[int],
    offset: int,
    concurrency: int,
    max_pages_per_site: int,
) -> None:
    all_restaurants = load_restaurants(restaurants_json)
    total_in_json = len(all_restaurants)

    # Apply offset & limit on raw list
    if offset:
        all_restaurants = all_restaurants[offset:]
    if limit is not None and limit > 0:
        all_restaurants = all_restaurants[:limit]

    restaurants = [r for r in all_restaurants if r.website]
    total_to_process = len(restaurants)

    print(f"[INFO] Loaded {total_in_json} restaurants from JSON.")
    print(f"[INFO] {total_to_process} have a website and will be processed (after offset/limit).")
    if limit is None:
        print("[INFO] No limit set: processing all available restaurants with websites in this segment.")
    else:
        print(f"[INFO] Limit set to {limit} entries (after offset).")

    ensure_dir(out_root)
    rp_cache: Dict[str, robotparser.RobotFileParser] = {}

    timeout = httpx.Timeout(20.0, connect=10.0)
    limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    }

    async with httpx.AsyncClient(
        timeout=timeout,
        headers=headers,
        limits=limits,
        follow_redirects=True,
        http2=False,
    ) as client:
        sem = asyncio.Semaphore(concurrency)
        progress_lock = asyncio.Lock()
        completed = 0

        async def worker(idx: int, rr: Restaurant) -> Dict[str, Any]:
            nonlocal completed
            async with sem:
                res = await process_restaurant(
                    client,
                    rp_cache,
                    out_root,
                    rr,
                    max_pages_per_site=max_pages_per_site,
                )
            async with progress_lock:
                completed += 1
                if res.get("skipped"):
                    status = "SKIP"
                elif res.get("found"):
                    status = "FOUND"
                else:
                    status = "MISS"
                print(f"[{completed}/{total_to_process}] {rr.name} ({status})")
            return res

        print(f"[INFO] Starting scrape with concurrency={concurrency} ...")
        results: List[Dict[str, Any]] = await asyncio.gather(
            *(worker(i, r) for i, r in enumerate(restaurants))
        )

    # Write summary CSV and JSON
    summary_csv = os.path.join(out_root, "scrape_results.csv")
    with open(summary_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["uuid", "slug", "name", "website", "found", "num_files", "errors", "skipped"])
        for res in results:
            errs = ";".join(res.get("errors", []))
            writer.writerow([
                res.get("uuid", ""),
                res.get("slug", ""),
                res.get("name", ""),
                res.get("website", ""),
                int(res.get("found", False)),
                len(res.get("saved_files", [])),
                errs,
                int(res.get("skipped", False)),
            ])

    summary_json = os.path.join(out_root, "scrape_results.json")
    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    found_count = sum(1 for r in results if r.get("found"))
    skipped_count = sum(1 for r in results if r.get("skipped"))
    print(
        f"[DONE] Processed {total_to_process} sites "
        f"(from first {limit if limit is not None and limit > 0 else 'all'} entries after offset)."
    )
    print(f"[DONE] Found menus for {found_count}/{total_to_process} restaurants "
          f"({skipped_count} skipped because menus already existed).")
    print(f"[DONE] Results saved under: {out_root}")
    print(f"[DONE] Summary CSV: {summary_csv}")
    print(f"[DONE] Summary JSON: {summary_json}")


def main():
    ap = argparse.ArgumentParser(
        description="Scrape restaurant menu HTML/PDF pages and save per-restaurant"
    )

    ap.add_argument(
        "--restaurants-json",
        default="phase2_restaurants_details.json",
        help="Path to restaurants JSON file",
    )

    # We will resolve this to be next to the JSON file
    ap.add_argument(
        "--out",
        default="menus",
        help="Output root directory (default: 'menus' next to the JSON file)",
    )

    # Default 0 = process ALL restaurants from JSON (no limit)
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of restaurants to process (0 = all, default = 0)",
    )

    ap.add_argument("--offset", type=int, default=0, help="Offset into the list")

    ap.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Concurrent fetches",
    )

    ap.add_argument(
        "--max-pages-per-site",
        type=int,
        default=5,
        help="Max menu-like pages saved per site",
    )

    args = ap.parse_args()

    # 0 → None → no limit
    limit = None if args.limit == 0 else args.limit

    # Resolve menus folder so it's next to the JSON file by default
    json_abs = os.path.abspath(args.restaurants_json)
    json_dir = os.path.dirname(json_abs)
    if args.out == "menus":
        out_root = os.path.join(json_dir, "menus")
    else:
        # If user gave a custom path, respect it (can be absolute or relative)
        out_root = args.out

    try:
        asyncio.run(
            run(
                restaurants_json=json_abs,
                out_root=out_root,
                limit=limit,
                offset=args.offset,
                concurrency=args.concurrency,
                max_pages_per_site=args.max_pages_per_site,
            )
        )
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
