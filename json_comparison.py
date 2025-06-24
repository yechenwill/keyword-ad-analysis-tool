from deepdiff import DeepDiff
import json

with open('C:/Users/ywang/Downloads/Opera_Autocomplete_04022025 (1).json') as f1, open('C:/Users/ywang/Desktop/Opera_Autocomplete_04172025.json') as f2:
    json1 = json.load(f1)
    json2 = json.load(f2)

# Get the differences
diff = DeepDiff(json1, json2, ignore_order=True)

# Save the diff to a new JSON file
with open('json_diff_output.json', 'w') as outfile:
    json.dump(diff, outfile, indent=4)

print("✅ Differences saved to 'C:/Users/ywang/Desktop/json_diff_output.json'")