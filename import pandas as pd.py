import pandas as pd

# Load the file
df = pd.read_csv("C:/Users/ywang/Desktop/Opera UK DE KWs index file - Index Kws - UK.tsv", sep="\t")

# List to store all rows (original + partials)
all_rows = []

# Iterate over original DataFrame rows
for _, row in df.iterrows():
    kw = str(row['KW']).strip()
    other_cols = row.drop('KW')

    # Add the original KW first
    all_rows.append({'KW': kw, **other_cols.to_dict()})

    words = kw.split()
    
    # Single-word KW
    if len(words) == 1:
        base = words[0]
        for i in range(3, len(base)):
            partial_kw = base[:i]
            all_rows.append({'KW': partial_kw, **other_cols.to_dict()})

    # Multi-word KW
    elif len(words) >= 2:
        first = words[0]
        second = words[1]
        for i in range(1, len(second)):
            partial_kw = f"{first} {second[:i]}"
            all_rows.append({'KW': partial_kw, **other_cols.to_dict()})

# Create a new DataFrame
df_with_partials = pd.DataFrame(all_rows)

# Save the result
df_with_partials.to_csv("KW_with_all_partials.tsv", sep="\t", index=False)
print("✅ All partials generated and saved to 'KW_with_all_partials.tsv'")
