# NFO Data Processing Pipeline (Streamlined)

This script automates the process of handling and converting NFO (NSE Futures and Options) tick-by-tick data into a more usable 1-second OHLC (Open, High, Low, Close) format.

This streamlined version is designed for simplicity. It uses a temporary directory for all intermediate steps (like multi-stage extraction and file renaming), which is **automatically created and deleted** upon completion. You only need to manage your input and output folders.

The pipeline performs the following process:
1.  **Unzips:** Extracts data archives in multiple stages into a hidden temporary folder.
2.  **Renames:** Renames the extracted files in the temporary folder.
3.  **Processes:** Converts the raw tick data into clean CSV files.
4.  **Cleans Up:** Deletes the temporary folder and all intermediate files.

## Final Output

The script generates the following files inside a date-stamped folder (e.g., `Output_CSV/2025-09-19/`):

-   `NIFTYBANK.csv`: A 1-second OHLC dataset for the NIFTY BANK index.
-   Multiple `BANKNIFTY...CE.csv` and `BANKNIFTY...PE.csv` files: 1-second OHLC datasets for the most relevant Call and Put options for that trading day.

## Requirements

-   **Python 3**: With the `pandas` library installed.
    ```sh
    pip install pandas
    ```
-   **7-Zip**: The 7-Zip command-line tool must be installed and accessible via the system's PATH. You can download it from [www.7-zip.org](https://www.7-zip.org/).

## How to Use

1.  **Folder Structure**: Your folder setup is now much simpler. Just have the script and your `Input_Zip` folder.

    ```
    allinone/
    ├── nfo_processor_streamlined.py
    ├── Input_Zip/
    │   └── your_initial_archive.zip
    └── Output_CSV/ (will be created)
    ```

2.  **Place Data**: Put your initial zipped data file(s) (e.g., `2025-09-19_tick_data.zip`) into the `Input_Zip` folder.

3.  **Run the Script**: Open a terminal or command prompt in the `allinone` directory and execute the script:
    ```sh
    python nfo_processor_streamlined.py
    ```

4.  **Check the Output**: The script will print its progress and will notify you when it's cleaning up the temporary files. You can find the final CSV files in the `Output_CSV` directory, organized by date.

## Configuration

If your folder names or 7-Zip path are different, you can easily change them in the `CONFIGURATION` section at the top of the `nfo_processor_streamlined.py` script.

```python
# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Please update these paths according to your folder structure.

# Source folder for the initial archives.
Input_Zip_PATH = "Input_Zip"

# The final output directory where the processed CSV files will be saved.
OUTPUT_PATH = "Output_CSV"

# Path to the 7-Zip executable.
# If "7z" is in your system's PATH, you can leave it as is.
# Otherwise, provide the full path, e.g., r"C:\Program Files\7-Zip\7z.exe"
SEVEN_ZIP_EXE = "7z"
```

-   `Input_Zip_PATH`: Where you put your initial archives.
-   `OUTPUT_PATH`: Where the final CSVs will be saved.
-   `SEVEN_ZIP_EXE`: The command or path for the 7-Zip executable.
