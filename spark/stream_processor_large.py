
import sys
from pathlib import Path

sys.path.insert(0, "/opt/spark/work-dir")

import stream_processor as processor


processor.RAW_TOPIC = "raw-events-large"
processor.CLEAN_TOPIC = "clean-events-large"
processor.INVALID_TOPIC = "invalid-events-large"

processor.CHECKPOINT_DIR = str(
    Path("/opt/spark/sample_output/checkpoints/data_quality_large")
)

processor.DATABASE_PATH = Path(
    "/opt/spark/sample_output/player_lookup_large.db"
)


if __name__ == "__main__":
    processor.main()
