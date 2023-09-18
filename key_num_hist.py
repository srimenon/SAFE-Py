import os
import json
import matplotlib.pyplot as plt

# Directory containing the previously collected JSON files
output_dir = 'requests_outputs'

# List to store ResultListCount values
result_list_counts = []

# Loop through JSON files in the directory
for filename in os.listdir(output_dir):
    if filename.endswith('.json'):
        file_path = os.path.join(output_dir, filename)
        with open(file_path, 'r') as json_file:
            try:
                data = json.load(json_file)
                result_list_count = data.get("ResultListCount", 0)
                result_list_counts.append(result_list_count)
            except json.JSONDecodeError:
                print(f"Error decoding JSON in file: {file_path}")

# Generate a bar chart
bin_edges = range(0, 200001, 1000)
bin_centers = [edge + 500 for edge in bin_edges[:-1]]
bin_values, _ = plt.hist(result_list_counts, bins=bin_edges)
plt.bar(bin_centers, bin_values, width=1000, edgecolor='k')
plt.xlabel('ResultListCount')
plt.ylabel('Frequency')
plt.title('Bar Chart of ResultListCount')
plt.show()