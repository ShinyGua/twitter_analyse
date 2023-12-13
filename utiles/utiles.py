import os
import glob
import pandas as pd
from pathlib import Path


def combine_excel_files(output_dir, output_file="all_data.xlsx"):
    """
    Scans the specified directory for all .xlsx files, combines them into a single Excel file,
    with each sheet named after the original file name.

    :param output_dir: The directory containing the .xlsx files to combine
    :param output_file: The name of the output file
    """
    # Create a pattern for .xlsx files
    pattern = os.path.join(output_dir, '*.xlsx')

    # Find all .xlsx files in the directory
    file_list = glob.glob(pattern)

    # Convert output_dir to Path object if it's not already
    if not isinstance(output_dir, Path):
        output_dir = Path(output_dir)

    output_file = output_dir.joinpath(output_file)

    # Check if there are files to process
    if not file_list:
        print("No Excel files found in the directory.")
        return

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for file in file_list:
            try:
                # Read the Excel file into a DataFrame using the openpyxl engine
                df = pd.read_excel(file, engine='openpyxl')

                # Use the file name (without extension) as the sheet name
                sheet_name = os.path.splitext(os.path.basename(file))[0]

                # Write the DataFrame to a new sheet in the output file
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            except Exception as e:
                print(f"Failed to process {file}: {e}")

    print(f"Combined Excel file saved as {output_file}")
