import zipfile
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class DataIngestor(ABC):
    @abstractmethod
    def ingest(self, path: Path) -> pd.DataFrame:
        """
        Ingests data from a given path and returns it as a pandas DataFrame
        Implementations should handle the details of how to read the data
        from the given path and return it in a DataFrame.
        """
        pass


class UnsupportedIngestor(DataIngestor):
    def ingest(self, path: Path) -> pd.DataFrame:
        """
        This gets called whenever a data source is not supported.
        """
        pass


class ZipIngestor(DataIngestor):
    def ingest(self, path: Path) -> pd.DataFrame:
        """
        Unzips a zip file and ingests all its contents into a DataFrame
        The DataFrame is the concatenation of all the DataFrames that can be
        ingested from the files in the zip

        Args:
            path (Path): The path to the zip file to ingest

        Returns:
            pd.DataFrame: A DataFrame containing the ingested data
        """
        extracted_path = path.parent / path.stem
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(extracted_path)

        found_files = [file for file in extracted_path.glob("**/*") if file.is_file()]
        if not found_files:
            raise FileNotFoundError(f"No files to unzip found in file: {path}")

        ingested = [get_ingestor(file).ingest(file) for file in found_files]

        return pd.concat(ingested)


class CSVIngestor(DataIngestor):
    def ingest(self, path: Path) -> pd.DataFrame:
        return pd.read_csv(path)


EXTENSION_TO_INGESTOR = {
    ".zip": ZipIngestor(),
    ".csv": CSVIngestor(),
}


def get_ingestor(path: Path) -> DataIngestor:
    extension = path.suffix
    if extension not in EXTENSION_TO_INGESTOR.keys():
        # TODO: change to logging
        print(f"Extension {extension} not supported")

    return EXTENSION_TO_INGESTOR.get(extension, UnsupportedIngestor())
