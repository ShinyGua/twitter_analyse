from pathlib import Path
import random
from time import sleep

import pandas as pd
import requests
from configs import parse_option

def check_linkedin_url(link_url):
    if 'www.linkedin.com/in' in link_url:
        return True
    else:
        return False


class GoogleSearch():
    def __init__(self, config):
        self.api_key = config.google.api_key
        self.tw_cse_id = config.google.tw_cse_id
        self.lk_cse_id = config.google.lk_cse_id

    def google_search(self, query, cse_id, **kwargs):
        sleep(random.uniform(1, 2))
        search_url = "https://www.googleapis.com/customsearch/v1"
        params = {
            'q': query,
            'key': self.api_key,
            'cx': cse_id
        }
        params.update(kwargs)
        response = requests.get(search_url, params=params)
        return response.json()

    def get_lk_url(self, query):
        results = self.google_search(query, self.lk_cse_id)
        linkedin_url = None
        if 'items' in results:
            for item in results['items']:
                link_url = item['link']
                if check_linkedin_url(link_url):
                    linkedin_url = link_url
                    break
        return linkedin_url

    def get_tw_url(self, query):
        results = self.google_search(query, self.tw_cse_id)
        twitter_url = None
        if 'items' in results:
            for item in results['items']:
                link_url = item['link']
                if 'twitter.com' in link_url:
                    twitter_url = link_url
                    break
        return twitter_url

if __name__ == '__main__':

    config = parse_option()
    google_search = GoogleSearch(config)

    df = pd.read_stata("datasets/4. people company merge drop for linkedin twitter link.dta")
    # df = df.iloc[0:100]
    # Initialize a new column for LinkedIn URLs
    df['LinkedIn_URL'] = None
    # df['Twitter_URL'] = None
    output_google_dir = Path("output/xlsx/google/")

    for i in range(len(df)):
        full_name = df.iloc[i]['Full_Name']
        companyname = df.iloc[i]['companyname']
        primary_position = df.iloc[i]['Primary_Position']

        # get the linkedin url
        query = f"{full_name} {companyname} {primary_position} linkedin"
        linkedin_url = google_search.get_lk_url(query)
        if linkedin_url is None:
            primary_position = primary_position if primary_position != "Chief Executive Officer" else "CEO"
            query = f"{full_name} {primary_position} linkedin"
            linkedin_url = google_search.get_lk_url(query)
        df.at[i, 'LinkedIn_URL'] = linkedin_url
        print(f"LinkedIn URL found for {full_name}: {linkedin_url}")

        # get the twitter url
        # query = f"{full_name} {companyname} {primary_position} twitter"
        # twitter_url = google_search.get_tw_url(query)
        # if twitter_url is None:
        #     primary_position = primary_position if primary_position != "Chief Executive Officer" else "CEO"
        #     query = f"{full_name} {primary_position} twitter"
        #     twitter_url = google_search.get_tw_url(query)
        # df.at[i, 'Twitter_URL'] = twitter_url
        # print(f"Twitter URL found for {full_name}: {twitter_url}")

        if i % 100 == 0:
            df.to_excel(output_google_dir.joinpath("output_file.xlsx"), index=False)
            print(f">>> Processed {i+1} rows.")

    # Save the DataFrame with LinkedIn URLs to .xlsx and .dta files
    df.to_excel(output_google_dir.joinpath("output_file.xlsx"), index=False)
    # df.to_stata("output_file.dta", write_index=False)
    print("Done.")
