import json
import hashlib
import os
import re
import sys
import time
import urllib3
from functools import lru_cache
import concurrent.futures

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TIMEOUT = int(os.environ.get("TIMEOUT", 15))
PROBE_TIMEOUT = int(os.environ.get("PROBE_TIMEOUT", 2))  # Short timeout for quick version probes
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (QtEmbedded; U; Linux; C) AppleWebKit/533.3 "
        "(KHTML, like Gecko) MAG200 stbapp ver: 2 rev: 250 Safari/533.3"
    ),
    "Accept-Encoding": "identity",
    "Accept": "*/*",
    "Connection": "keep-alive",
}
PORTAL_PATHS = (
    "portal.php",
    "server/load.php",
    "stalker_portal/server/load.php",
)
EMPTY_VALUES = {"", "0", "null", "none", "unknown", "n/a"}
NEGATIVE_MARKERS = (
    "access denied",
    "authorization failed",
    "invalid mac",
    "mac not found",
    "device not found",
    "stb denied",
    "blocked",
    "disabled",
    "expired",
    "denied",
    "not exists",
    "not found",
)

SERVERS_URL = os.environ.get(
    "SERVERS_URL",
    "https://raw.githubusercontent.com/tong4321/hub-me/refs/heads/main/_tools/servers.json",
)
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "servers.json")
GITHUB_OUTPUT = os.environ.get("GITHUB_OUTPUT")

# Concurrency tuning (can be overridden via environment variables)
DEFAULT_POOL_SIZE = int(os.environ.get("DEFAULT_POOL_SIZE", 50))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", 15))
MAX_SERVER_WORKERS = int(os.environ.get("MAX_SERVER_WORKERS", 8))  # Parallel servers


def build_session():
    """
    Build a requests.Session with connection pooling and automatic retries.
    Reuses TCP/TLS connections and handles transient network errors.
    """
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    # Retry strategy for transient errors (5xx)
    retries = Retry(total=2, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504))
    adapter = HTTPAdapter(pool_connections=DEFAULT_POOL_SIZE, pool_maxsize=DEFAULT_POOL_SIZE, max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def normalize_mac(mac):
    if not isinstance(mac, str):
        return None

    hex_value = re.sub(r"[^0-9A-Fa-f]", "", mac)
    if len(hex_value) != 12:
        return None

    return ":".join(hex_value[index : index + 2] for index in range(0, 12, 2)).upper()


def is_meaningful(value):
    if value is None:
        return False
    if isinstance(value, (list, tuple, dict, set)):
        return bool(value)

    text = str(value).strip()
    return bool(text) and text.lower() not in EMPTY_VALUES


def has_negative_text(value):
    if not is_meaningful(value):
        return False

    lowered = str(value).strip().lower()
    return any(marker in lowered for marker in NEGATIVE_MARKERS)


def parse_json_response(response):
    text = response.text.strip()
    if not text:
        return None

    try:
        return response.json()
    except ValueError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        try:
            return json.loads(text[start : end + 1])
        except ValueError:
            return None


def extract_payload(data):
    if not isinstance(data, dict):
        return {}

    payload = data.get("js")
    return payload if isinstance(payload, dict) else {}


def has_explicit_error(data, response_text=""):
    if not isinstance(data, dict):
        return has_negative_text(response_text)

    payload = data.get("js")
    if payload is None:
        return True

    if isinstance(payload, str):
        return has_negative_text(payload)

    if not isinstance(payload, dict):
        return False

    for key in ("error", "error_msg", "msg", "message", "reason"):
        if has_negative_text(payload.get(key)):
            return True

    for key in ("blocked", "expired", "disabled"):
        value = payload.get(key)
        if isinstance(value, bool) and value:
            return True
        if isinstance(value, (int, float)) and value not in (0,):
            return True
        if isinstance(value, str) and value.strip().lower() in {
            "1",
            "true",
            "yes",
            "blocked",
            "expired",
            "disabled",
        }:
            return True

    status = payload.get("status")
    if isinstance(status, str) and status.strip().lower() in {
        "blocked",
        "disabled",
        "expired",
        "denied",
        "error",
        "fail",
    }:
        return True

    return False


def build_endpoint(base_url, portal_path):
    base_url = base_url.rstrip("/")
    if base_url.lower().endswith(portal_path.lower()):
        return base_url
    return f"{base_url}/{portal_path}"


@lru_cache(maxsize=256)
def detect_portal_paths(base_url):
    base_url = base_url.rstrip("/")
    preferred = []

    # Use parallel requests to probe version URLs faster
    version_urls = (
        (f"{base_url}/c/version.js", "portal.php"),
        (f"{base_url}/stalker_portal/c/version.js", "stalker_portal/server/load.php"),
    )

    # Probe version URLs in parallel instead of sequentially
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {}
        for version_url, portal_path in version_urls:
            future = executor.submit(
                requests.get,
                version_url,
                headers=DEFAULT_HEADERS,
                timeout=PROBE_TIMEOUT,
                verify=False,
            )
            futures[future] = portal_path

        for future in concurrent.futures.as_completed(futures):
            try:
                response = future.result()
                if response.status_code == 200:
                    preferred.append(futures[future])
            except requests.RequestException:
                continue

    if "/stalker_portal" in base_url.lower():
        preferred.append("server/load.php")

    ordered_paths = []
    for portal_path in [*preferred, *PORTAL_PATHS]:
        if portal_path not in ordered_paths:
            ordered_paths.append(portal_path)

    return tuple(ordered_paths)


def portal_request(session, endpoint, action, cookies, extra_headers=None, **params):
    request_headers = {}
    if extra_headers:
        request_headers.update(extra_headers)

    try:
        response = session.get(
            endpoint,
            params={
                "type": "stb",
                "action": action,
                "JsHttpRequest": "1-xml",
                **params,
            },
            cookies=cookies,
            headers=request_headers or None,
            timeout=TIMEOUT,
            verify=False,
        )
    except requests.RequestException:
        return None, None

    if response.status_code != 200 or not response.text.strip():
        return None, response

    return parse_json_response(response), response


def build_device_identity(mac):
    serialnumber = hashlib.md5(mac.encode()).hexdigest().upper()
    sn = serialnumber[0:13]
    device_id = hashlib.sha256(sn.encode()).hexdigest().upper()
    device_id2 = hashlib.sha256(mac.encode()).hexdigest().upper()
    hw_version_2 = hashlib.sha1(mac.encode()).hexdigest()

    return {
        "sn": sn,
        "device_id": device_id,
        "device_id2": device_id2,
        "adid": hw_version_2,
    }


def build_cookies(mac, identity):
    return {
        "adid": identity["adid"],
        "debug": "1",
        "device_id2": identity["device_id2"],
        "device_id": identity["device_id"],
        "hw_version": "1.7-BD-00",
        "mac": mac,
        "sn": identity["sn"],
        "stb_lang": "en",
        "timezone": "America/Los_Angeles",
    }


def has_profile_evidence(profile_payload, mac):
    if not isinstance(profile_payload, dict) or not profile_payload:
        return False

    score = 0

    if normalize_mac(profile_payload.get("mac")) == mac:
        score += 2
    if is_meaningful(profile_payload.get("id")):
        score += 2
    if is_meaningful(profile_payload.get("name")):
        score += 1
    if is_meaningful(profile_payload.get("ls")):
        score += 1
    if is_meaningful(profile_payload.get("login")):
        score += 1

    stb_type = profile_payload.get("stb_type")
    if is_meaningful(stb_type) and str(stb_type).upper().startswith("MAG"):
        score += 1

    return score >= 3


def has_account_evidence(account_payload):
    if not isinstance(account_payload, dict) or not account_payload:
        return False

    for field in (
        "ls",
        "login",
        "phone",
        "fname",
        "tariff_plan",
        "account_balance",
        "expire_billing_date",
        "end_date",
        "max_online",
    ):
        if is_meaningful(account_payload.get(field)):
            return True

    return False


def check_portal(session, url):
    url = url.rstrip("/")
    probe_urls = [url, f"{url}/c/", f"{url}/stalker_portal/c/"]

    for portal_path in detect_portal_paths(url):
        probe_urls.append(build_endpoint(url, portal_path))

    seen = set()
    # Probe multiple URLs in parallel with short timeout for quick failure detection
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(probe_urls), MAX_WORKERS)) as executor:
        futures = {}
        for probe_url in probe_urls:
            if probe_url in seen:
                continue
            seen.add(probe_url)
            future = executor.submit(
                session.get,
                probe_url,
                timeout=PROBE_TIMEOUT,
                verify=False,
                allow_redirects=True,
            )
            futures[future] = probe_url

        for future in concurrent.futures.as_completed(futures):
            try:
                response = future.result()
                if 200 <= response.status_code < 400:
                    return True
            except requests.RequestException:
                continue

    return False


def check_mac(session, portal_url, mac):
    normalized_mac = normalize_mac(mac)
    if not normalized_mac:
        return False

    identity = build_device_identity(normalized_mac)
    cookies = build_cookies(normalized_mac, identity)

    for portal_path in detect_portal_paths(portal_url):
        endpoint = build_endpoint(portal_url, portal_path)
        handshake_data, handshake_response = portal_request(
            session,
            endpoint,
            "handshake",
            cookies,
            token="",
        )

        if not handshake_data or has_explicit_error(
            handshake_data,
            handshake_response.text if handshake_response else "",
        ):
            continue

        handshake_payload = extract_payload(handshake_data)
        token = handshake_payload.get("token")
        token_random = handshake_payload.get("random") or "0"
        if not token:
            continue

        auth_headers = {"Authorization": f"Bearer {token}"}
        if handshake_payload.get("random"):
            auth_headers["X-Random"] = str(handshake_payload["random"])

        sig = hashlib.sha256(str(token_random).encode()).hexdigest().upper()

        profile_data, profile_response = portal_request(
            session,
            endpoint,
            "get_profile",
            cookies,
            extra_headers=auth_headers,
            hd="1",
            ver=(
                "ImageDescription: 0.2.18-r23-250; "
                "ImageDate: Wed Aug 29 10:49:53 EEST 2018; PORTAL version: 5.3.1; "
                "API Version: JS API version: 343; STB API version: 146; "
                "Player Engine version: 0x58c"
            ),
            num_banks="2",
            sn=identity["sn"],
            stb_type="MAG250",
            client_type="STB",
            image_version="218",
            video_out="hdmi",
            device_id=identity["device_id2"],
            device_id2=identity["device_id2"],
            sig=sig,
            auth_second_step="1",
            hw_version="1.7-BD-00",
            not_valid_token="0",
            timestamp=str(round(time.time())),
            api_sig="262",
            prehash="0",
        )

        if not profile_data or has_explicit_error(
            profile_data,
            profile_response.text if profile_response else "",
        ):
            continue

        profile_payload = extract_payload(profile_data)
        if not has_profile_evidence(profile_payload, normalized_mac):
            continue

        account_payloads = []
        for action in ("get_main_info", "get_account_info"):
            extra_data, extra_response = portal_request(
                session,
                endpoint,
                action,
                cookies,
                extra_headers=auth_headers,
            )

            if not extra_data:
                continue
            if has_explicit_error(extra_data, extra_response.text if extra_response else ""):
                continue

            account_payloads.append(extract_payload(extra_data))

        if any(has_account_evidence(payload) for payload in account_payloads):
            return True

        if has_profile_evidence(profile_payload, normalized_mac):
            return True

    return False


def verify_server(server):
    portal_url = server.get("portal_url", "")
    if not portal_url:
        return False, None

    session = build_session()
    portal_works = check_portal(session, portal_url)

    valid_macs = []
    seen_macs = set()

    # Pre-normalize and deduplicate MACs
    macs_to_check = []
    for raw_mac in server.get("macs", []):
        normalized_mac = normalize_mac(raw_mac)
        if not normalized_mac or normalized_mac in seen_macs:
            continue
        seen_macs.add(normalized_mac)
        macs_to_check.append(normalized_mac)

    # Check MACs in parallel using a thread pool for better performance
    if macs_to_check:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(macs_to_check))) as executor:
            future_to_mac = {}
            for mac in macs_to_check:
                # Create a fresh session per worker for thread-safety
                future = executor.submit(check_mac, build_session(), portal_url, mac)
                future_to_mac[future] = mac

            for future in concurrent.futures.as_completed(future_to_mac):
                mac = future_to_mac[future]
                try:
                    if future.result():
                        valid_macs.append(mac)
                except Exception:
                    # Silently skip failed MAC checks
                    pass

    if not valid_macs:
        return portal_works, None

    return portal_works, valid_macs


def main():
    print(f"Fetching servers from: {SERVERS_URL}")

    response = requests.get(SERVERS_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    servers = data.get("servers", [])
    valid_servers = []

    print(f"Verificare {len(servers)} servere...")

    # MAJOR OPTIMIZATION: Verify servers in parallel instead of sequentially
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_SERVER_WORKERS) as executor:
        future_to_server = {}
        for server in servers:
            future = executor.submit(verify_server, server)
            future_to_server[future] = server

        for future in concurrent.futures.as_completed(future_to_server):
            server = future_to_server[future]
            try:
                portal_works, valid_macs = future.result()
                server_name = server.get('name', 'Unknown')
                server_url = server.get('portal_url', 'N/A')
                print(f"Verificare server: {server_name} - {server_url}")
                print(f"  Portal: {'OK' if portal_works else 'FAIL'}")

                if valid_macs is None:
                    print("  -> Server invalid sau toate MAC-urile nefunctionale - STERS")
                    continue

                server["macs"] = valid_macs
                valid_servers.append(server)
                print(f"  -> Server OK, {len(valid_macs)} MAC-uri valide")
            except Exception as e:
                print(f"  -> Error verificare server: {e}")
                continue

    data["servers"] = valid_servers

    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=4)

    print(f"\nTerminat: {len(valid_servers)}/{len(servers)} servere ramase")

    if GITHUB_OUTPUT:
        with open(GITHUB_OUTPUT, "a") as f:
            f.write(f"valid_servers={len(valid_servers)}\n")
            f.write(f"total_servers={len(servers)}\n")
            f.write(f"valid_macs={sum(len(s['macs']) for s in valid_servers)}\n")

    return len(valid_servers) > 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
