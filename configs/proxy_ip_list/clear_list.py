import json

# Load the JSON data from the file
with open('Free_Proxy_List.json', 'r') as file:
    data = json.load(file)

# Filter out the entries where the country is 'CN'
filtered_data = [item for item in data if item['country'] != 'CN']

# Save the filtered data to a new JSON file
with open('filtered_file.json', 'w') as file:
    json.dump(filtered_data, file, indent=4)

print(f"Saved filtered data to 'filtered_file.json'. Total entries: {len(filtered_data)}")