import argparse
import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, unquote

import requests
from bs4 import BeautifulSoup


# -------- Helpers for Google Places --------

def parse_name_lat_lng_from_gmaps_url(gmaps_url: str):
    """
    Try to extract restaurant name and (lat, lng) from a Google Maps place URL.

    Example:
    https://www.google.com/maps/place/Pedro's/@40.702525,-73.9865153,17z/...
    """
    try:
        parsed = urlparse(gmaps_url)
        path = parsed.path  # e.g. /maps/place/Pedro's/@40.702525,-73.9865153,17z
        match = re.search(r"/maps/place/(?P<name>[^/]+)/@(?P<lat>-?\d+\.\d+),(?P<lng>-?\d+\.\d+)", path)
        if not match:
            return None, None, None

        raw_name = match.group("name")
        name = unquote(raw_name).replace("+", " ")
        lat = float(match.group("lat"))
        lng = float(match.group("lng"))
        return name, lat, lng
    except Exception:
        return None, None, None


def call_google_places_nearby(name, lat, lng, api_key):
    """
    Use Places Nearby Search to get place_id and canonical name/website.
    """
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lng}",
        "radius": 200,  # small radius around the coordinates
        "keyword": name,
        "key": api_key,
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("results"):
        return None
    return data["results"][0]  # best match


def call_google_places_textsearch(name, api_key):
    """
    Fallback: Places Text Search if we couldn't parse lat/lng from URL.
    """
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": name,
        "key": api_key,
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("results"):
        return None
    return data["results"][0]


def call_google_place_details(place_id, api_key):
    """
    Get name + website for a place_id.
    """
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,website,url",
        "key": api_key,
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get("result", {})


def get_place_info_from_google(restaurant_url, api_key):
    """
    From a Google Maps restaurant_url, infer (name, website_url).
    """
    inferred_name, lat, lng = parse_name_lat_lng_from_gmaps_url(restaurant_url)

    place_result = None

    if inferred_name and lat is not None and lng is not None:
        # First try Nearby Search
        try:
            place_result = call_google_places_nearby(inferred_name, lat, lng, api_key)
        except Exception:
            place_result = None

    if place_result is None and inferred_name:
        # Fallback: Text Search by name
        try:
            place_result = call_google_places_textsearch(inferred_name, api_key)
        except Exception:
            place_result = None

    if place_result is None:
        return inferred_name or None, None  # best effort

    place_id = place_result.get("place_id")
    if not place_id:
        return inferred_name or place_result.get("name"), None

    details = {}
    try:
        details = call_google_place_details(place_id, api_key)
    except Exception:
        pass

    name = details.get("name") or place_result.get("name") or inferred_name
    website = details.get("website")
    return name, website


# -------- Helpers for menu discovery & download --------

def find_menu_url_on_site(website_url: str):
    """
    Heuristic:
    - If 'menu' already in the website_url path: treat as menu.
    - Otherwise, fetch homepage and look for <a> with 'menu' in href or text.
    """
    if website_url is None:
        return None

    # If URL itself clearly looks like a menu page, just use it.
    lower_url = website_url.lower()
    if "menu" in lower_url:
        return website_url

    try:
        resp = requests.get(website_url, timeout=20)
        resp.raise_for_status()
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Best-effort: same-domain menu links
    parsed_base = urlparse(website_url)
    base_domain = parsed_base.netloc

    candidates = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").strip().lower()
        href_lower = href.lower()

        if "menu" not in href_lower and "menu" not in text:
            continue

        # Normalize absolute vs relative
        parsed_href = urlparse(href)
        if not parsed_href.netloc:
            # relative link
            # Use base scheme + netloc
            abs_url = f"{parsed_base.scheme}://{base_domain}{href if href.startswith('/') else '/' + href}"
        else:
            abs_url = href

        # Keep only same-domain links (loosely)
        if base_domain in urlparse(abs_url).netloc:
            candidates.append(abs_url)

    # Pick the "shortest" candidate (often /menu/ or /menus/)
    if not candidates:
        return None

    candidates = sorted(candidates, key=len)
    return candidates[0]


def download_menu_html(menu_url: str, output_dir: Path, record_uuid: str):
    """
    Download the menu HTML and save it to menus_html/<uuid>.html
    """
    if not menu_url:
        return None, None

    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{record_uuid}.html"
    file_path = output_dir / filename

    resp = requests.get(menu_url, timeout=30)
    resp.raise_for_status()

    file_path.write_text(resp.text, encoding="utf-8")

    rel_path = Path("menus_html") / filename
    rel_path_for_json = str(rel_path).replace("/", "\\")
    return rel_path_for_json, str(file_path)


# -------- Main pipeline --------

def load_unique_restaurants_from_reviews(json_path: Path):
    """
    restaurants_reviews.json has many review rows; we only need one row per restaurant_id.
    Return dict: { restaurant_id: restaurant_url }
    """
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    result = {}
    for row in data:
        rid = row.get("restaurant_id")
        url = row.get("restaurant_url")
        if not rid or not url:
            continue
        if rid not in result:
            result[rid] = url
    return result


def main():
    parser = argparse.ArgumentParser(description="Scrape restaurant menus using Google API.")
    parser.add_argument(
        "--google-api-key",
        required=True,
        help="Google Places API key",
    )
    parser.add_argument(
        "--tripadvisor-api-key",
        required=False,
        default=None,
        help="TripAdvisor API key (reserved for future use, optional)",
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to restaurants_reviews.json",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output menus_metadata.json",
    )
    parser.add_argument(
        "--menus-dir",
        default="menus_html",
        help="Directory to store menu HTML files (default: menus_html)",
    )
    parser.add_argument(
        "--menu-base-url",
        required=True,
        help=(
            "Base URL for menu_file_url. Example: "
            "https://falconxoft.github.io/i.chef-files"
        ),
    )

    args = parser.parse_args()

    google_api_key = args.google_api_key
    input_path = Path(args.input)
    output_path = Path(args.output)
    menus_dir = Path(args.menus_dir)
    menu_base_url = args.menu_base_url.rstrip("/")

    restaurants = load_unique_restaurants_from_reviews(input_path)
    print(f"Found {len(restaurants)} unique restaurants in {input_path}")

    output_records = []
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    for restaurant_id, restaurant_url in restaurants.items():
        print(f"\nProcessing {restaurant_id} -> {restaurant_url}")

        try:
            # 1) Get name + website from Google Places
            name, website = get_place_info_from_google(restaurant_url, google_api_key)
            if not name:
                # Best-effort fallback: derive name from URL path
                inferred_name, _, _ = parse_name_lat_lng_from_gmaps_url(restaurant_url)
                name = inferred_name or restaurant_id

            print(f"  Name: {name}")
            print(f"  Website: {website}")

            # 2) Find menu URL
            menu_url = find_menu_url_on_site(website) if website else None
            if not menu_url:
                print("  WARNING: Could not find menu URL; skipping.")
                continue

            print(f"  Menu URL: {menu_url}")

            # 3) Download HTML menu
            menu_rel_path, _ = download_menu_html(
                menu_url=menu_url,
                output_dir=menus_dir,
                record_uuid=record_uuid,
            )
                

            if not menu_rel_path:
                print("  WARNING: Failed to download menu HTML; skipping.")
                continue

            # 4) Build metadata entry
            record_uuid = str(uuid.uuid4())
            menu_file_url = (
                f"{menu_base_url}/menus_html/{Path(menu_rel_path).name}"
            )

            record = {
                "uuid": record_uuid,
                "menu_url": menu_url,
                "menu_format": "HTML",
                "inserted_at": now_iso,
                "scraped_at": now_iso,
                "restaurant_name": name,
                "menu_file": menu_rel_path,  # e.g. "menus_html\\chefi-00003_Pedros.html"
                "menu_file_url": menu_file_url,
            }

            output_records.append(record)
            print(f"  Saved menu as {menu_rel_path}")
            print(f"  Menu file URL: {menu_file_url}")

        except Exception as e:
            print(f"  ERROR processing {restaurant_id}: {e}")
            continue

    # 5) Write all records to JSON
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(output_records, f, ensure_ascii=False, indent=2)

    print(f"\nDone. Wrote {len(output_records)} menu records to {output_path}")


if __name__ == "__main__":
    main()
