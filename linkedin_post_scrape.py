from pathlib import Path

import pandas as pd

from configs import parse_option
from linkedin_api import Linkedin
from utiles.utiles import combine_excel_files


def save_to_excel(data, output_dir):
    df = pd.DataFrame(data)
    output_file = output_dir / f"fromOrganization.xlsx"
    df.to_excel(output_file, index=False)
    print(f"Data saved to {output_file}")


if __name__ == '__main__':
    config = parse_option()
    linkedin = Linkedin(config)

    posts = linkedin.search_post("artificial intelligence", fromOrganization="11190", limit=-1)
    output_dir = Path("output/xlsx/linkedin/")
    output_dir.mkdir(parents=True, exist_ok=True)

    data = []
    for epoch, post in enumerate(posts):
        if post == []:
            continue
        post_user = post[0]
        post_link = post[1]
        post_text = post[2]
        post_num_like = post[3]
        post_num_comment = post[4]

        data.append({
            "post_user": post_user,
            "post_link": post_link,
            "post_text": post_text,
            "post_num_like": post_num_like,
            "post_num_comment": post_num_comment
        })

    save_to_excel(data, output_dir)

    # combine_excel_files(output_dir)
