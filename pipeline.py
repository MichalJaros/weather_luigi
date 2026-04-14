import datetime
import json
import os

import luigi


ARTIFACTS_DIR = "artifacts"


def stage_file(checksum: str, stage: str, filename: str) -> str:
    return os.path.join(ARTIFACTS_DIR, checksum, stage, filename)


def write_stage_metadata(checksum: str, stage: str, input_path: str, output_path: str, extra: dict | None = None) -> None:
    metadata = {
        "stage": stage,
        "checksum": checksum,
        "input_path": input_path.replace("\\", "/"),
        "output_path": output_path.replace("\\", "/"),
        "produced_at": datetime.datetime.now().isoformat(timespec="seconds"),
    }
    if extra:
        metadata.update(extra)

    metadata_path = stage_file(checksum, stage, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)


class ExtractTask(luigi.Task):
    checksum = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(stage_file(self.checksum, "extract", "extracted.json"))

    def run(self):
        print("RUNNING EXTRACT")

        raw_input_path = stage_file(self.checksum, "extract", "raw_input.json")

        with open(raw_input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        extracted = {
            "checksum": self.checksum,
            "temperature_celsius": raw_data["current_weather"]["temperature"],
            "windspeed": raw_data["current_weather"]["windspeed"],
        }

        with self.output().open("w") as f:
            json.dump(extracted, f, ensure_ascii=False, indent=2)

        write_stage_metadata(
            checksum=self.checksum,
            stage="extract",
            input_path=raw_input_path,
            output_path=self.output().path,
            extra={"description": "Wyciągnięto interesujące pola z raw_input.json"},
        )


class TransformTask(luigi.Task):
    checksum = luigi.Parameter()

    def requires(self):
        return ExtractTask(checksum=self.checksum)

    def output(self):
        return luigi.LocalTarget(stage_file(self.checksum, "transform", "transformed.json"))

    def run(self):
        print("RUNNING TRANSFORM")

        with self.input().open("r") as f:
            extracted = json.load(f)

        temperature_celsius = extracted["temperature_celsius"]
        temperature_kelvin = round(temperature_celsius + 273.15, 2)

        transformed = {
            "checksum": self.checksum,
            "temperature_celsius": temperature_celsius,
            "temperature_kelvin": temperature_kelvin,
            "windspeed": extracted["windspeed"],
        }

        with self.output().open("w") as f:
            json.dump(transformed, f, ensure_ascii=False, indent=2)

        write_stage_metadata(
            checksum=self.checksum,
            stage="transform",
            input_path=self.input().path,
            output_path=self.output().path,
            extra={"description": "Przeliczono temperaturę z Celsjuszy na Kelviny"},
        )


class FinalTask(luigi.Task):
    checksum = luigi.Parameter()

    def requires(self):
        return TransformTask(checksum=self.checksum)

    def output(self):
        return luigi.LocalTarget(stage_file(self.checksum, "final", "final.txt"))

    def run(self):
        print("RUNNING FINAL")

        with self.input().open("r") as f:
            transformed = json.load(f)

        with self.output().open("w") as f:
            f.write("PIPELINE FINISHED\n")
            f.write(f"checksum: {self.checksum}\n")
            f.write(f"temperature_celsius: {transformed['temperature_celsius']}\n")
            f.write(f"temperature_kelvin: {transformed['temperature_kelvin']}\n")
            f.write(f"windspeed: {transformed['windspeed']}\n")

        write_stage_metadata(
            checksum=self.checksum,
            stage="final",
            input_path=self.input().path,
            output_path=self.output().path,
            extra={"description": "Zapisano końcowe podsumowanie pipeline"},
        )