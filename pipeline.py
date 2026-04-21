import hashlib
import json
import os

import luigi
import requests



class ExtractTask(luigi.Task):
    run_id = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join("output","extracted.json"))

    def calculate_checksum(self):
        output_path = self.output().path
        with open(output_path, "rb") as output_file:
            checksum = hashlib.file_digest(output_file, "sha256").hexdigest()
        checksum_path = f"{output_path}.checksum"
        with open(checksum_path, "w") as checksum_file:
            json.dump({"run_id": self.run_id, "checksum": checksum}, checksum_file)

    def get_checksum(self):
        checksum_path = f"{self.output().path}.checksum"
        try:
            with open(checksum_path) as checksum_file:
                checksum_data = json.load(checksum_file)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return None

        if checksum_data.get("run_id") != self.run_id:
            return None

        return checksum_data.get("checksum")

    def run(self):
        with self.output().open("w") as fo:
            response = requests.get("https://api.open-meteo.com/v1/forecast?latitude=52.23&longitude=22.01&current_weather=true", headers={}, timeout=30)
            response.raise_for_status()
            json.dump(response.json(), fo)
        self.calculate_checksum()

    def complete(self):
        output_exists = super().complete()
        if not output_exists:
            return False
        return self.get_checksum() is not None


class TransformTask(luigi.Task):
    run_id = luigi.Parameter()

    def requires(self):
        return ExtractTask(run_id=self.run_id)

    def output(self):
        return luigi.LocalTarget(os.path.join("output","transformed.json"))

    def calculate_checksum(self):
        checksum_path = f"{self.output().path}.checksum"
        extract_task = self.requires()
        with open(f"{extract_task.output().path}.checksum") as checksum_file:
            checksum_data = json.load(checksum_file)
        with open(checksum_path, "w") as checksum_file:
            json.dump({"run_id": self.run_id, "checksum": checksum_data["checksum"]}, checksum_file)

    def get_checksum(self):
        checksum_path = f"{self.output().path}.checksum"
        try:
            with open(checksum_path) as checksum_file:
                checksum_data = json.load(checksum_file)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return None

        if checksum_data.get("run_id") != self.run_id:
            return None

        return checksum_data.get("checksum")

    def run(self):
        with self.input().open("r") as fi:
            extracted = json.load(fi)
            temperature_celsius = extracted["current_weather"]["temperature"]
            temperature_kelvin = round(temperature_celsius + 273.15, 2)
            transformed = {
                "temperature_celsius": temperature_celsius,
                "temperature_kelvin": temperature_kelvin,
                "windspeed": extracted["current_weather"]["windspeed"],
            }
            with self.output().open("w") as fo:
                json.dump(transformed, fo, ensure_ascii=False, indent=2)
        self.calculate_checksum()

    def complete(self):
        output_exists = super().complete()
        if not output_exists:
            return False
        dependencies_completed = all(task.complete() for task in luigi.task.flatten(self.requires()))
        if not dependencies_completed:
            return False
        return self.get_checksum() == self.requires().get_checksum()


class FinalTask(luigi.Task):
    run_id = luigi.Parameter()

    def requires(self):
        return TransformTask(run_id=self.run_id)

    def output(self):
        return luigi.LocalTarget(os.path.join("output","final.json"))

    def run(self):
        with self.input().open("r") as fi:
            transformed = json.load(fi)
            with self.output().open("w") as fo:
                fo.write("PIPELINE FINISHED\n")
                fo.write(f"temperature_celsius: {transformed['temperature_celsius']}\n")
                fo.write(f"temperature_kelvin: {transformed['temperature_kelvin']}\n")
                fo.write(f"windspeed: {transformed['windspeed']}\n")

    def complete(self):
        output_exists = super().complete()
        if not output_exists:
            return False
        return all(task.complete() for task in luigi.task.flatten(self.requires()))
