import datetime
import json
import os

import luigi

from download_api import compute_checksum, download_json_from_api, stage_downloaded_payload
from pipeline import FinalTask


REGISTRY_PATH = "processed_registry.json"


def load_registry() -> dict:
    if not os.path.exists(REGISTRY_PATH):
        return {}

    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_registry(registry: dict) -> None:
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)


def main():
    print("KROK 1: Pobieram dane z API...")
    data = download_json_from_api()

    print("KROK 2: Liczę checksum...")
    checksum = compute_checksum(data)
    print(f"CHECKSUM: {checksum}")

    registry = load_registry()

    if checksum in registry:
        print("Ta sama zawartość była już wcześniej przetwarzana.")
        print("Pipeline Luigi nie zostanie uruchomiony.")
        return

    print("KROK 3: Tworzę katalog przebiegu i zapisuję raw_input.json...")
    run_metadata = stage_downloaded_payload(data, checksum)

    print("KROK 4: Uruchamiam cały pipeline Luigi...")
    luigi.build(
        [FinalTask(checksum=checksum)],
        local_scheduler=True,
        workers=1,
    )

    registry[checksum] = {
        "processed_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "source": run_metadata["source"],
        "raw_input_path": run_metadata["raw_input_path"],
    }
    save_registry(registry)

    print("KROK 5: Checksum zapisany do rejestru.")
    print("Pipeline wykonany poprawnie.")


if __name__ == "__main__":
    main()