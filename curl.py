import os
import json
import requests
import matplotlib.pyplot as plt

# Create a directory to store output files
output_dir = 'requests_outputs'
os.makedirs(output_dir, exist_ok=True)

# Lists to store ResultListCount values
result_list_counts = []

# Initial payload and URL
payload = {
    "ResultSetSize": 50,
    "ResultSetOffset": 0,
    "QueryGroups": [
        {
            "QueryRules": [
                {
                    "RuleType": "Simple",
                    "Values": ["0"],
                    "Columns": ["Event.ID"],
                    "Operator": "is greater than",
                    "overrideColumn": "",
                },
                {
                    "RuleType": "Simple",
                    "Values": ["1000"],
                    "Columns": ["Event.ID"],
                    "Operator": "is less than",
                    "overrideColumn": "",
                },
            ],
            "AndOr": "and",
            "inLastSearch": False,
            "editedSinceLastSearch": False,
        }
    ],
    "AndOr": "and",
    "SortColumn": None,
    "SortDescending": True,
    "TargetCollection": "cases",
    "SessionId": 326157,
}

base_url = "https://data.ntsb.gov/carol-main-public/api/Query/Main"

# Make 200 requests with incremented values
for i in range(200):
    # Update payload values
    payload["QueryGroups"][0]["QueryRules"][0]["Values"] = [str(i * 1000 - 1)]
    payload["QueryGroups"][0]["QueryRules"][1]["Values"] = [str((i + 1) * 1000)]

    # Construct the URL and headers
    url = base_url
    headers = {"Content-Type": "application/json"}

    # Make the POST request
    response = requests.post(url, json=payload, headers=headers)

    # Parse the JSON response
    response_data = response.json()

    # Extract the ResultListCount value
    result_list_count = response_data.get("ResultListCount", 0)

    # Append to the list
    result_list_counts.append(result_list_count)

    # Define the output filename
    output_filename = os.path.join(output_dir, f'output_{i}.json')

    # Save the response content to the output file
    with open(output_filename, 'w') as output_file:
        json.dump(response_data, output_file, indent=2)

    print(f'Request {i} complete. Output saved to {output_filename}')

# Generate a histogram plot
plt.hist(result_list_counts, bins=20, edgecolor='k')
plt.xlabel('ResultListCount')
plt.ylabel('Frequency')
plt.title('Histogram of ResultListCount')
plt.show()