import uuid
import luigi
from pipeline import FinalTask


def main():
    run_id = uuid.uuid4().hex
    luigi.build(
        [FinalTask(run_id=run_id)],
        local_scheduler=True,
        workers=1,
    )


if __name__ == "__main__":
    main()
