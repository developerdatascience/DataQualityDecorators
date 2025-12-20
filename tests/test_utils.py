import os
import importlib.util

# Load the module directly from the file to avoid importing the package
# (which pulls in pyspark during package import).
utils_path = os.path.join(os.getcwd(), "quality", "pipelines", "utils", "utils.py")
spec = importlib.util.spec_from_file_location("dq_utils", utils_path)
dq_utils = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dq_utils)
persist_bad_records = dq_utils.persist_bad_records


class MockWriter:
    def __init__(self):
        self.mode_arg = None
        self.written_path = None

    def mode(self, arg):
        self.mode_arg = arg
        return self

    def parquet(self, path):
        self.written_path = path


class MockDF:
    def __init__(self):
        self.write = MockWriter()


def test_persist_bad_records_creates_path_and_writes(tmp_path):
    df = MockDF()
    base = str(tmp_path / "dq_bad_records")
    dataset = "datasetA"
    exp = "exp1"

    persist_bad_records(df, dataset, exp, base_path=base)

    expected = f"{base}/{dataset}/{exp}"
    assert df.write.mode_arg == "overwrite"
    assert df.write.written_path == expected
    assert os.path.isdir(expected)
