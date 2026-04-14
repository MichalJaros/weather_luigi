import datetime
import hashlib
import json
import os

import requests
from dotenv import load_dotenv


load_dotenv()

API_URL = os.getenv(
    "API_URL",
    "https://api.open-meteo.com/v1/forecast?latitude=52.23&longitude=21.01&current_weather=true",
)
API_KEY = os.getenv("API_KEY")
ARTIFACTS_DIR = "artifacts"


def canonical_json_bytes(data: dict) -> bytes:
    return json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def compute_checksum(data: dict) -> str:
    return hashlib.sha256(canonical_json_bytes(data)).hexdigest()


def download_json_from_api(url: str = API_URL) -> dict:
    headers = {}

    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def ensure_run_dirs(checksum: str) -> str:
    run_dir = os.path.join(ARTIFACTS_DIR, checksum)
    os.makedirs(os.path.join(run_dir, "extract"), exist_ok=True)
    os.makedirs(os.path.join(run_dir, "transform"), exist_ok=True)
    os.makedirs(os.path.join(run_dir, "final"), exist_ok=True)
    return run_dir


def stage_downloaded_payload(data: dict, checksum: str) -> dict:
    run_dir = ensure_run_dirs(checksum)

    raw_input_path = os.path.join(run_dir, "extract", "raw_input.json")
    with open(raw_input_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    run_metadata = {
        "checksum": checksum,
        "source": "open-meteo",
        "downloaded_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "raw_input_path": raw_input_path.replace("\\", "/"),
    }

    with open(os.path.join(run_dir, "run_metadata.json"), "w", encoding="utf-8") as f:
        json.dump(run_metadata, f, ensure_ascii=False, indent=2)

    return run_metadata