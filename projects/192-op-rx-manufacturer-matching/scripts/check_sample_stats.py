import pandas as pd

# Load sample and RX manufacturers
sample_df = pd.read_csv('data/input/op_manufacturers_sample100.csv')
rx_df = pd.read_csv('data/input/rx_manufacturers.csv')

print(f'Test population size: {len(sample_df)} OP manufacturers')
print(f'Matching against: {len(rx_df)} RX manufacturers')
print(f'Total potential comparisons: {len(sample_df) * len(rx_df):,}')
print()

# With blocking
def get_blocking_key(name):
    if pd.isna(name) or not name:
        return 'UNKNOWN'
    name = str(name).strip().upper()
    for prefix in ['THE ', 'A ', 'AN ']:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    if name and name[0].isalpha():
        return name[0]
    elif name and name[0].isdigit():
        return 'NUMERIC'
    else:
        return 'SPECIAL'

sample_df['blocking_key'] = sample_df['manufacturer_name'].apply(get_blocking_key)
rx_df['blocking_key'] = rx_df['manufacturer_name'].apply(get_blocking_key)

# Calculate comparisons with blocking
total_comparisons = 0
for block in sample_df['blocking_key'].unique():
    sample_count = len(sample_df[sample_df['blocking_key'] == block])
    rx_count = len(rx_df[rx_df['blocking_key'] == block])
    total_comparisons += sample_count * rx_count

reduction = 1 - (total_comparisons / (len(sample_df) * len(rx_df)))

print('With blocking strategy:')
print(f'  Total comparisons: {total_comparisons:,}')
print(f'  Reduction: {reduction:.1%}')
print()
print('Estimated for Tier2 with LLM:')
print(f'  Fuzzy-only matches (>85%): ~{int(total_comparisons * 0.25):,}')
print(f'  API calls needed (<85%): ~{int(total_comparisons * 0.75):,}')
print(f'  Estimated runtime: ~{int(total_comparisons * 0.75 / 60)} minutes at 60 calls/min')
print(f'  Estimated cost: ~${total_comparisons * 0.75 * 0.15 / 1000:.2f}')