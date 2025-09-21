
import os
import subprocess
import pandas as pd
import pickle
import tempfile
import shutil

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Please update these paths according to your folder structure.

# Source folder for the initial archives.
# Place your .zip, .7z, or .rar archives here.
DATA1_PATH = "Input_Zip"

# The final output directory where the processed CSV files will be saved.
# A date-stamped subfolder will be created here.
OUTPUT_PATH = "Output_CSV"

# Path to the 7-Zip executable.
# If "7z" is in your system's PATH, you can leave it as is.
# Otherwise, provide the full path, e.g., r"C:\Program Files\7-Zip\7z.exe"
SEVEN_ZIP_EXE = "7z"


# ==============================================================================
# STAGE 1: UNZIP ARCHIVES
# ==============================================================================

def extract_archives_with_7zip(source_dir, target_dir, stage_name):
    """
    Scans a source directory for archives and extracts them to a target directory using 7-Zip.
    """
    print("---" * 10)
    print(f"Executing Extraction Stage: '{stage_name}'")
    print(f"Source: '{source_dir}' -> Target: '{target_dir}'")
    
    if not os.path.isdir(source_dir):
        print(f"❌ Error: The source folder '{source_dir}' does not exist.")
        return False

    os.makedirs(target_dir, exist_ok=True)
    
    print(f"📂 Scanning for archives in '{source_dir}'...")
    
    found_archives = False
    for file_name in os.listdir(source_dir):
        if file_name.endswith(('.zip', '.7z', '.rar')):
            found_archives = True
            archive_path = os.path.join(source_dir, file_name)
            print(f"📦 Extracting {file_name}...")

            command = [SEVEN_ZIP_EXE, 'x', archive_path, f'-o{target_dir}', '-y']
            
            try:
                result = subprocess.run(command, check=True, capture_output=True, text=True, shell=True)
                print(f"  ✅ -> Successfully extracted {file_name}")
            except FileNotFoundError:
                print(f"  ❌ -> FATAL ERROR: '{SEVEN_ZIP_EXE}' command not found.")
                print("       Please ensure 7-Zip is installed and its folder is in your system's PATH,")
                print(f"       or update the SEVEN_ZIP_EXE variable in this script.")
                return False
            except subprocess.CalledProcessError as e:
                print(f"  ❌ -> Error extracting {file_name}.")
                print(f"       7-Zip Error Message: {e.stderr}")
                return False

    if not found_archives:
        print("🤷 No archive files found in this folder.")
        
    print(f"✨ Extraction stage '{stage_name}' is complete!")
    print("---" * 10 + "\n")
    return True

# ==============================================================================
# STAGE 2: RENAME FILES
# ==============================================================================

def rename_files(directory):
    """
    Renames files in a directory by swapping the date and name parts.
    Example: 'tick_data_2025-09-19.pkl' -> '2025-09-19_tick_data'
    """
    print("---" * 10)
    print(f"Executing Stage: Renaming files in '{directory}'")

    if not os.path.isdir(directory):
        print(f"❌ Error: The directory '{directory}' does not exist.")
        return False
        
    for filename in os.listdir(directory):
        try:
            # We assume the format is NAME_YYYY-MM-DD.pkl
            remainder, date_part = filename.rsplit('_', 1)
            date_part = date_part.replace('.pkl', '')
            new_filename = f"{date_part}_{remainder}"
            
            old_filepath = os.path.join(directory, filename)
            new_filepath = os.path.join(directory, new_filename)

            if not os.path.exists(new_filepath):
                os.rename(old_filepath, new_filepath)
                print(f"Renamed: {filename} -> {new_filename}")
            else:
                print(f"Skipped renaming (already exists): {new_filename}")

        except ValueError:
            print(f"Skipped: '{filename}' does not match the expected format for renaming.")
    
    print("✨ Renaming task is complete!")
    print("---" * 10 + "\n")
    return True

# ==============================================================================
# STAGE 3: PROCESS NFO DATA
# ==============================================================================

def process_nfo_data(data_dir, output_dir):
    """
    Processes instrument and tick data to generate 1-second OHLC CSV files
    for NIFTYBANK and relevant BANKNIFTY options.
    """
    print("---" * 10)
    print(f"Executing Stage: Processing data from '{data_dir}' to '{output_dir}'")

    if not os.path.isdir(data_dir):
        print(f"❌ Error: The data directory '{data_dir}' does not exist.")
        return False

    instrument_files = [f for f in os.listdir(data_dir) if f.endswith('_instrument_df')]
    tick_data_files = [f for f in os.listdir(data_dir) if f.endswith('_tick_data')]

    if not instrument_files or not tick_data_files:
        print(f"🤷 No instrument or tick data files found in '{data_dir}'.")
        return False

    for instrument_file in instrument_files:
        date_part = instrument_file.split('_')[0]
        corresponding_tick_file = f"{date_part}_tick_data"

        if corresponding_tick_file in tick_data_files:
            print(f"Processing data for date: {date_part}")
            
            instrument_path = os.path.join(data_dir, instrument_file)
            tick_data_path = os.path.join(data_dir, corresponding_tick_file)

            with open(instrument_path, 'rb') as f:
                instrument_df = pickle.load(f)
            with open(tick_data_path, 'rb') as f:
                tick_data = pickle.load(f)

            process_niftybank(instrument_df, tick_data, date_part, output_dir)
            process_banknifty_options(instrument_df, tick_data, date_part, output_dir)
        else:
            print(f"No corresponding tick data file found for {instrument_file}")
            
    print("✨ NFO data processing is complete!")
    print("---" * 10 + "\n")
    return True

def create_ohlc_df(data, date_str):
    """Helper function to create a 1-second OHLC DataFrame."""
    df = pd.DataFrame(data, columns=["Timestamp", "Price"])
    df["Timestamp"] = pd.to_datetime(df["Timestamp"]).dt.floor("s")
    
    start_time = "09:15:00"
    end_time = "15:29:59"
    full_range = pd.date_range(start=f"{date_str} {start_time}", end=f"{date_str} {end_time}", freq="s")
    
    ohlc = df.groupby("Timestamp").agg(
        Open=("Price", "first"),
        High=("Price", "max"),
        Low=("Price", "min"),
        Close=("Price", "last"),
    ).reset_index()
    
    ohlc_full = pd.DataFrame(full_range, columns=["Timestamp"]).merge(ohlc, on="Timestamp", how="left")
    
    ohlc_full[["Open", "High", "Low", "Close"]] = ohlc_full[["Open", "High", "Low", "Close"]].fillna(method="ffill")
    ohlc_full[["Open", "High", "Low", "Close"]] = ohlc_full[["Open", "High", "Low", "Close"]].fillna(method="bfill")

    return ohlc_full

def process_niftybank(instrument_df, tick_data, date_part, output_base_path):
    """Extracts, processes, and saves NIFTYBANK index data."""
    print("  -> Processing NIFTYBANK index...")
    condition = (
        (instrument_df['tradingsymbol'] == 'NIFTY BANK') &
        (instrument_df['name'] == 'NIFTY BANK') &
        (instrument_df['instrument_type'] == 'EQ') &
        (instrument_df['segment'] == 'INDICES')
    )
    nifty_bank_instrument = instrument_df[condition]

    if nifty_bank_instrument.empty:
        print("     - NIFTY BANK instrument not found.")
        return

    token = nifty_bank_instrument['instrument_token'].iloc[0]
    
    if token not in tick_data:
        print(f"     - Tick data for NIFTY BANK (Token: {token}) not found.")
        return

    nifty_data = tick_data[token]
    ohlc_df = create_ohlc_df(nifty_data, date_part)

    date_folder_path = os.path.join(output_base_path, date_part)
    os.makedirs(date_folder_path, exist_ok=True)
    
    output_file_path = os.path.join(date_folder_path, "NIFTYBANK.csv")
    ohlc_df.to_csv(output_file_path, index=False)
    print(f"     - Saved NIFTYBANK.csv to {output_file_path}")

def process_banknifty_options(instrument_df, tick_data, date_part, output_base_path):
    """Finds relevant BANKNIFTY options, processes, and saves them."""
    print("  -> Processing BANKNIFTY options...")
    
    niftybank_csv_path = os.path.join(output_base_path, date_part, "NIFTYBANK.csv")
    if not os.path.exists(niftybank_csv_path):
        print("     - NIFTYBANK.csv not found, cannot determine strike range for options.")
        return
        
    niftybank_df = pd.read_csv(niftybank_csv_path)
    max_tick = niftybank_df['High'].max()
    min_tick = niftybank_df['Low'].min()

    center_price_max = round(max_tick / 100) * 100 + 200
    center_price_min = round(min_tick / 100) * 100 - 200
    strikes = range(center_price_min, center_price_max + 1, 100)
    
    rounded_prices = [f"{strike}{opt_type}" for strike in strikes for opt_type in ['CE', 'PE']]

    options_condition = (instrument_df['name'] == 'BANKNIFTY') & (instrument_df['exchange'] == 'NFO')
    banknifty_options = instrument_df[options_condition]
    
    if banknifty_options.empty:
        print("     - No BANKNIFTY options found in instrument file.")
        return
        
    min_expiry = banknifty_options['expiry'].min()
    nearest_expiry_options = banknifty_options[banknifty_options['expiry'] == min_expiry]

    pattern = '|'.join([f"{price}$" for price in rounded_prices])
    
    filtered_rows = nearest_expiry_options[
        nearest_expiry_options['tradingsymbol'].str.contains(pattern, regex=True)
    ]

    token_to_tradingsymbol = dict(zip(filtered_rows['instrument_token'], filtered_rows['tradingsymbol']))

    for token, symbol in token_to_tradingsymbol.items():
        if token in tick_data:
            option_tick_list = tick_data[token]
            
            if option_tick_list and isinstance(option_tick_list[0], (list, tuple)) and len(option_tick_list[0]) >= 2:
                option_data_for_ohlc = [[item[0], item[1]] for item in option_tick_list]
                ohlc_df = create_ohlc_df(option_data_for_ohlc, date_part)
            
                date_folder_path = os.path.join(output_base_path, date_part)
                os.makedirs(date_folder_path, exist_ok=True)
                
                output_file_path = os.path.join(date_folder_path, f"{symbol}.csv")
                ohlc_df.to_csv(output_file_path, index=False)
                print(f"     - Saved {symbol}.csv")
            else:
                print(f"     - Data for {symbol} (Token: {token}) has an unexpected format.")

        else:
            print(f"     - Tick data for {symbol} (Token: {token}) not found.")

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def main():
    """
    Main function to run the entire NFO data processing pipeline using a temporary directory.
    """
    print("🚀 Starting Streamlined NFO Data Processing Pipeline...")
    
    temp_dir = tempfile.mkdtemp()
    print(f"Created temporary directory: {temp_dir}")
    
    # Define paths for intermediate steps inside the temporary directory
    temp_data2 = os.path.join(temp_dir, 'data2')
    temp_data3 = os.path.join(temp_dir, 'data3')

    try:
        # Stage 1: Unzip initial data from DATA1_PATH to temp_data2
        if not extract_archives_with_7zip(DATA1_PATH, temp_data2, "Initial Extraction"):
            print("Pipeline stopped due to extraction error.")
            return

        # Stage 2: Unzip intermediate data from temp_data2 to temp_data3
        if not extract_archives_with_7zip(temp_data2, temp_data3, "Intermediate Extraction"):
            print("Pipeline stopped due to extraction error.")
            return
            
        # Stage 3: Rename the extracted files for processing in temp_data3
        if not rename_files(temp_data3):
            print("Pipeline stopped due to renaming error.")
            return

        # Stage 4: Process the data from temp_data3 and generate CSVs in OUTPUT_PATH
        if not process_nfo_data(temp_data3, OUTPUT_PATH):
            print("Pipeline stopped due to data processing error.")
            return
            
        print("✅✅✅ All tasks completed successfully! ✅✅✅")

    finally:
        # Clean up the temporary directory and all its contents
        print(f"\n🧹 Cleaning up temporary directory: {temp_dir}")
        shutil.rmtree(temp_dir)
        print("✨ Cleanup complete.")


if __name__ == "__main__":
    main()