from pathlib import Path

import pandas as pd

from configs import parse_option
from linkedin_api import Linkedin


def save_to_excel(data, output_dir):
    df = pd.DataFrame(data)
    output_file = output_dir / f"company_data.xlsx"
    df.to_excel(output_file, index=False)
    print(f"Data saved to {output_file}")


if __name__ == '__main__':
    config = parse_option()
    linkedin = Linkedin(config)

    profile = linkedin.get_company("sequoia")

    data = []
    username = profile.get("name")
    user_description = profile.get("tagline")
    stage = profile.get("companyIndustries")[0].get("localizedName")
    confirmedLocations = profile.get("confirmedLocations")[0]
    location = f"{confirmedLocations.get('city')}, " \
               f"{confirmedLocations.get('geographicArea')}, " \
               f"{confirmedLocations.get('country')}"
    staffCountRange = profile.get("staffCountRange")
    num_employees = f"{staffCountRange.get('start')} - {staffCountRange.get('end')}"

    entityUrn = profile.get("entityUrn").split(":")[-1]
    data.append({
        "username": username,
        "user_description": user_description,
        "stage": stage,
        "location": location,
        "num_employees": num_employees,
        "entityUrn": entityUrn
    })
    output_dir = Path("output/xlsx/linkedin/")
    output_dir.mkdir(parents=True, exist_ok=True)
    save_to_excel(data, output_dir)
