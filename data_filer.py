import glob
import os
import time
from pathlib import Path

import pandas as pd
from pandas import DataFrame

from configs import parse_option


class CompanyDatasets():
    def __init__(self, config):
        # Create the output directory
        self.output_dir = Path(config.output_dir)
        self.output_xlsx = self.output_dir.joinpath('xlsx')
        self.output_dta = self.output_dir.joinpath('dta')
        self.output_csv = self.output_dir.joinpath('csv')
        self.output_xlsx.mkdir(parents=True, exist_ok=True)
        self.output_dta.mkdir(parents=True, exist_ok=True)
        self.output_csv.mkdir(parents=True, exist_ok=True)

        # get the column names from config for xlsx
        self.output_company_columns_names = config.output_company_columns_names
        self.output_deal_columns_names = config.output_deal_columns_names

        # record the time
        self.time_columns = time.time()

    def print_column_names(self):
        # Print the column names
        print("\nColumn names in Company dataset:")
        print(self.df_company.columns.tolist())
        print("\nColumn names in Company Industry Relation dataset:")
        print(self.df_company_industry_relation.columns.tolist())
        print("\nColumn names in Deal dataset:")
        print(self.df_deal.columns.tolist())

    def load_data(self):
        """
        Load the data from the .dta files
        :return:
        """
        company_path = config.datasets.company_path
        company_industry_relation_path = config.datasets.company_industry_relation_path
        deal_path = config.datasets.deal_path

        # Read the .dta files
        print(">>> Loading Company datasets...")
        self.df_company = pd.read_stata(company_path)
        self.time_cost()
        print(">>> Loading Company Industry Relation datasets...")
        self.df_company_industry_relation = pd.read_stata(company_industry_relation_path)
        self.time_cost()
        print(">>> Loading Deal datasets...")
        self.df_deal = pd.read_stata(deal_path)
        self.df_deal = self.df_deal.drop('companyname', axis=1).drop('businessstatus', axis=1)
        self.time_cost()

    def save_as_xlsx_dta_csv(self, df: DataFrame, file_name: str):
        """
        Save the DataFrame as an Excel file, a Stata file, and a CSV file.
        :param df:
        :param file_name:
        :return:
        """
        print(f">>> Saving {file_name}...")

        # Save the DataFrame as an Excel file
        with pd.ExcelWriter(self.output_xlsx.joinpath(f"{file_name}.xlsx"), mode="w") as writer:
            df4execl = self.filter_for_save(df)
            df4execl.to_excel(writer, index=False)
        # Save the DataFrame as a CSV file
        df.to_csv(self.output_csv.joinpath(f"{file_name}.csv"), index=False)
        # Save the DataFrame as a Stata file
        df.to_stata(self.output_dta.joinpath(f"{file_name}.dta"), write_index=False, version=117)

        print(f">>> Saved {file_name}")

    def combine_excel_files(self, output_file="all_data.xlsx"):
        """
        Scans the specified directory for all .xlsx files, combines them into a single Excel file,
        with each sheet named after the original file name.

        :param directory: Directory to scan for Excel files.
        :param output_file: Path of the output Excel file.
        """
        # Create a pattern for .xlsx files
        pattern = os.path.join(self.output_xlsx.__str__(), '*.xlsx')

        # Find all .xlsx files in the directory
        file_list = glob.glob(pattern)
        output_file = self.output_xlsx.joinpath(output_file)

        with pd.ExcelWriter(output_file) as writer:
            for file in file_list:
                # Read the Excel file into a DataFrame
                df = pd.read_excel(file)

                # Use the file name (without extension) as the sheet name
                sheet_name = os.path.splitext(os.path.basename(file))[0]

                # Write the DataFrame to a new sheet in the output file
                df.to_excel(writer, sheet_name=sheet_name, mode="r+", index=False)
        self.time_cost()

    def filter_for_save(self, output: DataFrame) -> DataFrame:
        # Select specific columns
        save_columns = self.output_deal_columns_names + self.output_company_columns_names
        return output[save_columns]

    def filter_1_adn_2(self, file_name: str = "US and USD with twitter") -> DataFrame:
        """
        Filter 1) hdquarter: US & native currency of deal: US Dollars (USD)
        Filter 2) twitter account =! Empty
        Output 1: company in US with twitter account
        """
        # Applying Filter 1: US & twitterprofileurl =! Empty
        filter_1_company = self.df_company[(self.df_company['hqcountry'].str.lower() == 'United States'.lower()) &
                                           ((self.df_company['twitterprofileurl'] != '') &
                                            (self.df_company['twitterprofileurl'].notna()))]

        # Applying Filter 2: USD
        filter_1_deal = self.df_deal[self.df_deal['nativecurrencyofdeal'].str.lower() == 'US Dollars (USD)'.lower()]

        # Merge filtered data on companyid to ensure alignment
        filter_1_and_2_data = pd.merge(filter_1_company, filter_1_deal, on='companyid', how='inner').drop_duplicates()

        self.save_as_xlsx_dta_csv(filter_1_and_2_data, file_name)
        self.time_cost()
        return filter_1_and_2_data

    def filter_3(self, df: DataFrame, file_name: str = "at least two financing") -> DataFrame:
        """
        Filter 3) Dealno. > = 2 (note: if the company id has showed up in the dealno >=2, keep it’s dealno.=1 record)
        and dealstatus: completed
        Output 2: company in US with twitter account with at least two financing rounds
        """

        # Filtering based on deal conditions
        # Ensure that 'companyid' is a common column in both dataframes
        # Group df_deal by 'companyid' and filter for those with at least two deals
        deal_counts = df['companyid'].value_counts()

        # Further filter for companies with at least one 'completed' deal
        companies_with_two_or_more_deals = deal_counts[deal_counts >= 2].index.tolist()

        # Final filter for companies that meet all criteria
        filter_3_data = df[df['companyid'].isin(companies_with_two_or_more_deals)].drop_duplicates()

        self.save_as_xlsx_dta_csv(filter_3_data, file_name)
        self.time_cost()
        return filter_3_data

    def filter_4(self, df: DataFrame, file_name: str = "with valuation") -> DataFrame:
        """
        Filter 4) pre-money valuation or post valuation =! Empty
        Output 3: company in US with twitter account with at least two financing rounds and with valuation information
        """

        # Filter for companies with valuation information
        filter_4_data = df[((df['premoneyvaluation'].notna()) & (df['premoneyvaluation'] != '')) |
                           ((df['postvaluation'].notna()) & (df['postvaluation'] != ''))
                           ].drop_duplicates()

        self.save_as_xlsx_dta_csv(filter_4_data, file_name)
        self.time_cost()
        return filter_4_data.drop_duplicates()

    def filter_5(self, df: DataFrame, file_name: str = "VC or PE") -> DataFrame:
        """
        Filter 5) deal class: either venture capital or private equity, otherwise drop
        Output 4: company in US with twitter account with at least two financing rounds and with valuation information
        in private equity or venture capital
        """

        # Filter for companies with 'venture capital' or 'private equity' deal class
        filter_5_data = df[(df['dealclass'].str.lower() == 'venture capital') |
                           (df['dealclass'].str.lower() == 'private equity')
                           ].drop_duplicates()

        self.save_as_xlsx_dta_csv(filter_5_data, file_name)
        self.time_cost()
        return filter_5_data.drop_duplicates()

    def filter_6(self, df: DataFrame, file_name: str = "dealstatus = completed") -> DataFrame:
        """
        Filter 6) Exclude null or empty 'dealdate' and include 'completed' deal status
        :param df:
        :return:
        """

        # Filter for companies with 'completed' deal status and non-empty 'dealdate'
        filter_6_data = df[(df['dealdate'].notna()) &
                           (df['dealdate'] != '') &
                           (df['dealstatus'].str.lower() == 'completed')
                           ].drop_duplicates()

        self.save_as_xlsx_dta_csv(filter_6_data, file_name)
        self.time_cost()
        return filter_6_data.drop_duplicates()

    def filter(self):
        self.load_data()

        # Filter 1) hdquarter: US & native currency of deal: US Dollars (USD)
        # Filter 2) twitter account =! Empty
        filter_1_and_2_data = self.filter_1_adn_2()

        # Filter 3) Companies with at least two financing rounds
        filter_3_data = self.filter_3(filter_1_and_2_data)

        # Filter 4) pre-money valuation or post valuation =! Empty
        filter_4_data = self.filter_4(filter_3_data)

        # Filter 5) deal class: either venture capital or private equity, otherwise drop
        filter_5_data = self.filter_5(filter_4_data)

        # Filter 6: Exclude null or empty 'dealdate' and include 'completed' deal status
        filter_6_data = self.filter_6(filter_5_data)

        # Applying Filter 3 again: Companies with at least two financing rounds
        final_data = self.filter_3(filter_6_data, file_name="final")

        # Combine all the Excel files into a single file
        self.combine_excel_files()

        print(">>> Finished filtering")

    def count_unique_company_ids(self, file_name="all_data.xlsx"):
        # Load the Excel file
        file_path = self.output_xlsx.joinpath(file_name)
        if not file_path.exists():
            print(f">>> File {file_path} does not exist")
            return
        xls = pd.ExcelFile(file_path)

        # Iterate through each sheet
        for sheet_name in xls.sheet_names:
            # Read the sheet into a DataFrame
            df = pd.read_excel(xls, sheet_name)

            # Assuming 'companyid' is the column name for company IDs
            unique_count = df['companyid'].nunique()

            # Print the count of unique company IDs for each sheet
            print(f">>> Sheet: {sheet_name}, Unique Company IDs: {unique_count}")
        self.time_cost()

    def time_cost(self):
        use_time = time.time() - self.time_columns
        print(f">>> Time cost:{use_time:.2f} seconds")
        self.time_columns = time.time()


if __name__ == '__main__':
    config = parse_option()

    company_datasets = CompanyDatasets(config)

    # Do the filtering as required
    # company_datasets.filter()
    # company_datasets.combine_excel_files()
    # Count the number of unique company IDs in each sheet
    company_datasets.count_unique_company_ids()
