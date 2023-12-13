import os
from pathlib import Path

import pandas as pd


def read_file(file_path):
    """
    Read the Excel file
    :param file_path: the file path
    :return: the dataframe
    """
    if file_path.endswith('.xlsx'):
        df = pd.read_excel(file_path)
    elif file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        raise ValueError(f"File type of {file_path} is not supported.")
    return df

if __name__ == '__main__':
    # Read the Excel files
    file1 = 'datasets/Match/2. SDC M&A DATA 2000 - 2023 Filter (Private , US).xlsx'  # Replace with your file path
    file2 = 'datasets/Match/4. Pitchbook Data - M&A (Universe VC, dealtype MA, CURRENTY USD, LOCATION US).xlsx'  # Replace with your file path

    df1 = read_file(file1)
    df2 = read_file(file2)

    df1_column_name = "Target Name"
    # df2_column_name = "CompanyName"
    df2_column_name = "Pronucleotein Biotechnologies"
    output_name = "Merge 2&4"

    erase_list = ['Inc', 'Ltd', 'LLC', 'Corp', f' \(']
    for item in erase_list:
        if df1.get('Target Name 2') is not None:
            df1[f'{df1_column_name} 2'] = df1[f'{df1_column_name} 2'].str.split(item, expand=False).str[0]
            df2[f'{df2_column_name} 2'] = df2[f'{df2_column_name} 2'].str.split(item, expand=False).str[0]
        else:
            df1[f'{df1_column_name} 2'] = df1[df1_column_name].str.split(item, expand=False).str[0]
            df2[f'{df2_column_name} 2'] = df2[df2_column_name].str.split(item, expand=False).str[0]

    df1[f'{df1_column_name} 2'] = df1[f'{df1_column_name} 2'].str.strip().str.lower()
    df2[f'{df2_column_name} 2'] = df2[f'{df2_column_name} 2'].str.strip().str.lower()

    # Merge the dataframes on the company names
    merged_df = pd.merge(df1, df2, left_on=f'{df1_column_name} 2', right_on=f'{df2_column_name} 2', how='inner')
    merged_df.drop_duplicates(subset=[f'{df1_column_name} 2', f'{df2_column_name} 2'], inplace=True)
    merged_df.drop_duplicates(inplace=True)

    # Save the merged dataframe to a new Excel file
    output_path = f'output/{output_name}.xlsx'
    output_path = Path(output_path)
    merged_df.to_excel(output_path, index=False)

