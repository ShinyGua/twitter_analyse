import os
import random
import time
from datetime import datetime

import pandas as pd
import requests

from configs import parse_option


# os.environ["http_proxy"] = "http://45.32.131.86:3000"
# os.environ["https_proxy"] = "https://45.32.131.86:3000"

class Gtwitter():

    def __init__(self, config):
        self.headers = dict()

        self.headers["x-twitter-active-user"] = config.tw.header.x_twitter_active_user
        self.headers["x-twitter-auth-type"] = config.tw.header.x_twitter_auth_type
        self.headers["x-twitter-client-language"] = config.tw.header.x_twitter_client_language
        self.headers["referer"] = config.tw.header.referer
        self.headers["user-agent"] = config.tw.header.user_agent
        self.headers["accept"] = config.tw.header.accept

        self.headers["x-csrf-token"] = config.tw.header.x_csrf_token
        self.headers["cookie"] = config.tw.header.cookie
        self.headers["authorization"] = config.tw.header.authorization

    def checnkut(self, masturl):
        try:
            username = str(masturl.split('com/')[-1].split("/")[0]).lower()
            self.username = username
            api = 'https://twitter.com/i/api/graphql/gr8Lk09afdgWo7NvzP89iQ/UserByScreenName'
            params = {
                'variables': '{"screen_name":"' + username + '","withSafetyModeUserFields":true,"withSuperFollowsUserFields":true}'
            }
            data = self.getblog(params, api)
            if data is None:
                return None, None, None

            userid = data.get("data").get("user").get("result").get("rest_id")
            statuses_count = self.find_key(data, "statuses_count", userid)
            created_at = self.find_key(data, "created_at", userid)
            statuses_count = statuses_count if statuses_count is not None else -1
            created_at = created_at if created_at is not None else -1
            print(f">>> {masturl}: {statuses_count}, {created_at}")
            return userid, statuses_count, created_at
        except Exception as e:
            print(f">>> 未查找到用户名： {masturl} {e} ")
            if "'NoneType' object has no attribute 'get'" in e.__str__():
                return None, -1, -1
            return None, None, None

    def change(self, text):
        try:
            dt_obj = datetime.strptime(text, '%a %b %d %H:%M:%S %z %Y')
            return dt_obj.astimezone(tz=None).strftime('%Y-%m-%d %H:%M:%S')

        except Exception as e:
            print(f">>> 时间格式异常： {text}")
            return text

    def getblog(self, params, search_api):
        error_num = 0
        while error_num < 3:
            try:
                # Generate a random sleep duration between 1 to 5 seconds
                random_sleep_duration = random.uniform(1, 10)

                # Sleep for the random duration
                time.sleep(random_sleep_duration)
                res = requests.get(
                    search_api,
                    headers=self.headers,
                    params=params,
                    timeout=(3, 4)
                )
                return res.json()

            except Exception as e:
                print(f">>> parse error: {e}")
                time.sleep(5 * 60)  # sleep 5 minutes
                error_num += 1
        return None

    def find_key(self, data, key, userid, curren_id=-1):
        if isinstance(data, dict):
            for k, v in data.items():
                if k == "rest_id":
                    curren_id = v
                if k == key and curren_id == userid:
                    return v
                if isinstance(v, (dict, list)):
                    result = self.find_key(v, key, userid, curren_id)
                    if result is not None:
                        return result
        elif isinstance(data, list):
            for item in data:
                result = self.find_key(item, key, userid, curren_id)
                if result is not None:
                    return result
        return None

    def update_excel_with_twitter_data(self, input_file, sheet_name, output_file="output/xlsx/post_num.xlsx"):
        # Read the input Excel file
        input_df = pd.read_excel(input_file, sheet_name=sheet_name)

        # Drop duplicates based on companyid and twitterprofileurl from input file
        unique_df = input_df.drop_duplicates(subset=['companyid', 'twitterprofileurl'])

        # Check if the output file exists and read it
        if os.path.exists(output_file):
            output_df = pd.read_excel(output_file)
        else:
            output_df = pd.DataFrame(columns=['companyid', 'twitterprofileurl', 'statuses_count', 'created_at'])

        # Initialize counter for processed rows
        counter = 0
        e_time = time.time()
        # Iterate through each row in the unique DataFrame
        for index, row in unique_df.iterrows():
            companyid = row['companyid']
            url = row['twitterprofileurl']

            # Save the file every config.save_frequency rows processed
            if counter % config.tw.save_frequency == 0 and counter != 0:
                output_df.to_excel(output_file, index=False)
                cost_time = time.time() - e_time
                e_time = time.time()
                est_time = cost_time * (len(unique_df) - counter) / config.tw.save_frequency / 60
                print(f">>> {counter} pieces of data have been processed, estimated remaining {est_time:.2f} Minutes")

            # Skip the row if statuses_count and created_at are already filled in output file
            if not output_df[(output_df['companyid'] == companyid) &
                             ((pd.notna(output_df['statuses_count'])) |
                              (output_df['statuses_count'] == ""))].empty:
                counter += 1
                continue

            _, statuses_count, created_at = self.checnkut(url)

            # Append the results to the output DataFrame
            if companyid not in output_df['companyid'].values:
                # if the companyid is not in the output_df, add status_count and created_at
                output_df.loc[len(output_df)] = [companyid, url, statuses_count, created_at]
            else:
                # if the companyid is in the output_df, update status_count and created_at
                output_df.loc[output_df['companyid'] == companyid, ['statuses_count', 'created_at']] = [statuses_count,
                                                                                                        created_at]

            counter += 1

        # Save any remaining changes after the loop
        output_df.to_excel(output_file, index=False)


if __name__ == '__main__':
    config = parse_option()
    tt = Gtwitter(config)

    # tt.blockdata("https://twitter.com/apolloglobal")
    tt.update_excel_with_twitter_data("output/xlsx/all_data.xlsx",
                                      "final",
                                      "output/xlsx/post_num.xlsx")
