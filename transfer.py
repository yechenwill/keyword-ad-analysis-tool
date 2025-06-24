input_file = 'C:/Users/ywang/Documents/Codes/raw_keywords.csv'
output_file = 'C:/Users/ywang/Documents/Codes/raw_keywords——transfered.csv'

with open(input_file, 'r', encoding='cp1252') as src, \
     open(output_file, 'w', encoding='utf-8') as dst:
    contents = src.read()
    dst.write(contents)

print("✅ File successfully converted to UTF-8.")
