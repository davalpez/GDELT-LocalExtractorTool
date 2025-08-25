# GDELT Local Extractor

<!-- Optional: Add some badges here for a professional touch. You can find them on shields.io -->
<!-- ![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg) -->
<!-- ![License](https://img.shields.io/badge/license-MIT-green.svg) -->

A command-line tool designed to efficiently download, process, and consolidate data from the [GDELT Project](https://www.gdeltproject.org/). It leverages PySpark to handle large volumes of data and transforms it into local, query-friendly formats like CSV and Parquet.

This tool was created to simplify the initial data engineering phase of GDELT analysis, allowing researchers and data scientists to focus on generating insights rather than data wrangling.

## Key Features

-   **Automated Downloads:** Fetch GDELT 1.0 data files for a specified date range.
-   **Parallel Processing:** Utilizes a local PySpark session to process files in parallel, significantly speeding up data transformation.
-   **Flexible Output:** Save processed data as consolidated CSV or highly efficient Parquet files.
-   **Configurable:** Easily customize output paths, data columns, and processing settings through a simple configuration file.
-   **Command-Line Interface:** Easy to use and integrate into automated data pipelines.

## Installation

Follow these steps to set up the tool and its environment.

**Prerequisites:**
*   Python 3.9+ ( I used 3.12 )
*   Java 17+ (required for PySpark, I have openjdk version "21.0.8" )

**Steps:**

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd GDELTLocalExtractor
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    # For Linux/macOS
    python3 -m venv venv
    source venv/bin/activate

    # For Windows
    python -m venv venv
    .\venv\Scripts\activate
    ```

3.  **Install the project and its dependencies:**
    This command reads the `pyproject.toml` file and installs the tool along with all required libraries.
    ```bash
    pip install .
    ```
    *For developers who intend to modify the source code, install in editable mode:*
    ```bash
    pip install -e .

### 1. Configuration

Before the first run, you must configure your settings in the `config.py` file located in the project's root directory. This file controls where data is downloaded and stored, and which data columns you wish to keep.
As a default, the folders for downloaded data are automatically created in this repository folder. The only parameters adviced to be changed are FILTER_TERMS and FILTER_TERMS_COLUMNS if the user wishes to filder the content from GDELT towards a specific subject.

In /GDELTLocalExtractor/DataExtractionTool/utils/schema.py, there is a list of columns I have decided to use/prioritize when filtering columns, which can be also changed/modified by the user.


### 2. Execution

Run the tool from the root of the project directory. The main command structure is:

```bash
python -m DataExtractionTool.GDELT_Extractor [options]
```

#### Command-Line Arguments

The tool accepts the following arguments to control its behavior:

| Argument | Description | Required / Default |
| :--- | :--- | :--- |
| `-s, --start_date` | The start date for the data range in **YYYY-MM-DD** format. | **Required** |
| `-e, --end_date` | The end date for the data range in **YYYY-MM-DD** format. | **Required** |
| `--chunk_size` | The number of days to download and process in each batch. Helps manage memory for very large date ranges. | Default: `5` |
| `-f, --filter` | A flag that, when present, activates the filtering logic defined in `config.py`. | Disabled |
| `-u, --only_download_and_unzip` | A special mode that only downloads the raw GDELT files and unzips them. **It will NOT run the Spark processing pipeline** (no filtering, no Parquet/consolidated CSV output). | Disabled |

---
### Examples

#### Basic Extraction

Download and process all GDELT data from February 1st, 2024, to February 3rd, 2024. This will run the full Spark pipeline but will not apply the filters from `config.py`.

```bash
python -m DataExtractionTool.GDELT_Extractor --start_date "2024-02-01" --end_date "2024-02-03"
```

#### Extraction with Filtering

Run the full Spark pipeline and **apply the `FILTER_CONDITIONS`** specified in your `config.py` file.

```bash
python -m DataExtractionTool.GDELT_Extractor -s "2024-02-01" -e "2024-02-03" -f
```

#### Download-Only Mode

Quickly fetch the raw data for a date range without processing it. This is useful for archiving or manual inspection. This command will **not** start a Spark session.

```bash
python -m DataExtractionTool.GDELT_Extractor -s "2024-02-01" -e "2024-02-03" -u
```

#### Advanced Extraction

Process data for a large date range, applying filters and using a smaller chunk size to manage system resources effectively.

```bash
python -m DataExtractionTool.GDELT_Extractor -s "2024-01-01" -e "2024-03-31" -f --chunk_size 2
```

---


## Testing

This project uses the `pytest` framework to ensure the reliability and correctness of its core logic.

To run the tests, first install the project's development dependencies (which includes `pytest`):

```bash
# Make sure you are in your project's root directory with venv activated
pip install .[dev]
```

Then, you can run the full test suite with a single command:

```bash
pytest
```

#### Current Coverage

Currently, the tests are focused on the most critical part of the data pipeline: the Spark transformations. The test file `test/test_spark_transforms.py` contains unit tests that validate the functions within the `utils/spark_transforms.py` module, ensuring that data is processed correctly.

This is a foundational test suite, and the goal is to expand coverage in the future to include other utility modules such as the downloader, file handler, and input validation to further guarantee the tool's robustness.

---

## Project Structure

```
.
├── DataExtractionTool
├── GDELT_Extractor.py
├── __pycache__
│   └── GDELT_Extractor.cpython-312.pyc
├── test
│   ├── __init__.py
│   ├── artifacts
│   ├── test_data
│   │   ├── sample_gdelt_data.CSV
│   │   ├── sample_gdelt_lookup.txt
│   │   └── sample_manual_lookup.csv
│   └── test_spark_transforms.py
└── utils
    ├── __init__.py
    ├── downloader.py
    ├── file_handler.py
    ├── input_validation.py
    ├── logger_config.py
    ├── schema.py
    ├── spark_manager.py
    └── spark_transforms.py

├── assets
│   ├── MASTER-GDELTDOMAINSBYCOUNTRY-MAY2018.txt
│   ├── cameo_dictionary
│   ├── cameo_dictionary:Zone.Identifier
│   ├── extended_lookup.csv
│   └── gdelt_headers.xlsx
├── config.py
├── data
│   ├── gdelt_downloaded_data
│   └── merged_parquet
├── pyproject.toml
├── test.ipynb
├── config.py                    # User-configurable settings (MUST EDIT)
├── pyproject.toml               # Project definition and dependencies
├── .gitignore                   # Files and folders to ignore in Git
└── README.md                    # This file
```

## Contributing

Contributions are welcome! If you have suggestions for improvements or find a bug, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
```




