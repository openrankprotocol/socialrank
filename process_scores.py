#!/usr/bin/env python3
"""
Score Processing Script

This script processes score files from the scores/ directory by:
1. Loading all CSV score files
2. Treating all peers as Discord users (usernames only)
3. Applying transformations to make exponential distributions more linear
4. Normalizing scores so all scores sum to 1
5. Saving results to output/ directory with transformation suffixes

Transformations available:
- Square Root: sqrt transformation for gentle compression of higher values
- Logarithmic: log transformation (first scaled to 1-10 range) to linearize exponential data
- Quantile: uniform distribution preserving rank order

Usage:
    python3 process_scores.py

Requirements:
    - pandas (install with: pip install pandas)
    - numpy (install with: pip install numpy)
    - scipy (for quantile transformation)
    - CSV files in scores/ directory with columns 'i' (identifier) and 'v' (score)

Output:
    - Creates output/ directory if it doesn't exist
    - For each input file (e.g., ai.csv), creates:
      - {filename}_users_sqrt.csv: Users with square root transformation
      - {filename}_users_log.csv: Users with logarithmic transformation (scaled 1-10 first)
      - {filename}_users_quantile.csv: Users with quantile transformation
    - Scores are normalized (sum to 1) and sorted by score (descending)
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
import re
import argparse

from scipy import stats


def normalize_scores(df):
    """
    Normalize scores so that all scores sum to 1, then map to 100-1000 range

    Args:
        df (pandas.DataFrame): DataFrame with 'v' column containing scores

    Returns:
        pandas.DataFrame: DataFrame with normalized scores mapped to 100-1000 range
    """
    if len(df) == 0:
        return df

    df_normalized = df.copy()
    total_score = df['v'].sum()

    # Avoid division by zero if all scores are zero
    if total_score == 0:
        df_normalized['v'] = 1.0 / len(df)  # Equal distribution
    else:
        df_normalized['v'] = df['v'] / total_score

    # Map to 100-1000 range
    df_normalized['v'] = df_normalized['v'] * 900 + 100

    # Round to 2 decimal places
    df_normalized['v'] = df_normalized['v'].round(2)

    return df_normalized


def apply_sqrt_transformation(df):
    """Apply square root transformation to scores"""
    if len(df) == 0:
        return df

    df_transformed = df.copy()

    # Apply sqrt transformation
    df_transformed['v'] = np.sqrt(df['v'])

    # Normalize to 0-1 range
    min_val = df_transformed['v'].min()
    max_val = df_transformed['v'].max()
    if max_val != min_val:
        df_transformed['v'] = (df_transformed['v'] - min_val) / (max_val - min_val)
    else:
        df_transformed['v'] = 1.0 / len(df)

    # Map to 100-1000 range
    df_transformed['v'] = df_transformed['v'] * 900 + 100

    # Round to 2 decimal places
    df_transformed['v'] = df_transformed['v'].round(2)

    return df_transformed


def apply_log_transformation(df):
    """Apply logarithmic transformation to scores (first scale to 1-10, then log)"""
    if len(df) == 0:
        return df

    df_transformed = df.copy()

    # First normalize to 0-1 range
    min_val = df['v'].min()
    max_val = df['v'].max()
    if max_val != min_val:
        df_transformed['v'] = (df['v'] - min_val) / (max_val - min_val)
    else:
        df_transformed['v'] = 1.0 / len(df)

    # Map to 1-10 range
    df_transformed['v'] = df_transformed['v'] * 9 + 1

    # Apply log transformation
    df_transformed['v'] = np.log(df_transformed['v'])

    # Normalize back to 0-1 range
    min_log = df_transformed['v'].min()
    max_log = df_transformed['v'].max()
    if max_log != min_log:
        df_transformed['v'] = (df_transformed['v'] - min_log) / (max_log - min_log)
    else:
        df_transformed['v'] = 1.0 / len(df)

    # Map to 100-1000 range
    df_transformed['v'] = df_transformed['v'] * 900 + 100

    # Round to 2 decimal places
    df_transformed['v'] = df_transformed['v'].round(2)

    return df_transformed

def apply_quantile_transformation(df):
    """Apply quantile-based uniform distribution transformation"""
    if len(df) == 0:
        return df

    df_transformed = df.copy()

    # Use scipy for quantile transformation
    df_transformed['v'] = stats.rankdata(df['v']) / len(df['v'])

    # Map to 100-1000 range
    df_transformed['v'] = df_transformed['v'] * 900 + 100

    # Round to 2 decimal places
    df_transformed['v'] = df_transformed['v'].round(2)

    return df_transformed


def load_user_ids_mapping(scores_file):
    """
    Load username to user ID mapping from raw/user_ids_[server_name].csv

    Args:
        scores_file (str): Path to the scores CSV file

    Returns:
        dict: Mapping of username to user_id, or empty dict if file not found
    """
    try:
        # Extract server name from scores file
        scores_path = Path(scores_file)
        base_name = scores_path.stem

        # Look for user_ids mapping file in raw/ folder
        raw_folder = Path("raw")
        user_ids_files = list(raw_folder.glob(f"user_ids_*.csv"))

        if not user_ids_files:
            print(f"    Warning: No user_ids mapping files found in raw/ folder")
            return {}

        # Use the first mapping file found (assuming one server at a time)
        mapping_file = user_ids_files[0]
        print(f"    Loading username mapping from: {mapping_file}")

        username_to_id = {}
        with open(mapping_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()[1:]  # Skip header
            for line in lines:
                line = line.strip()
                if line:
                    # Handle quoted usernames containing commas
                    if line.startswith('"'):
                        # Find the closing quote
                        closing_quote = line.find('"', 1)
                        if closing_quote != -1:
                            username = line[1:closing_quote]
                            user_id = line[closing_quote + 2:]  # Skip quote and comma
                        else:
                            continue
                    else:
                        parts = line.split(',', 1)
                        if len(parts) == 2:
                            username, user_id = parts
                        else:
                            continue

                    username_to_id[username] = user_id

        print(f"    Loaded {len(username_to_id)} username mappings")
        return username_to_id

    except Exception as e:
        print(f"    Warning: Could not load username mapping: {str(e)}")
        return {}


def process_scores(input_file, output_dir, use_user_ids=False):
    """
    Process a single score file by applying transformations and saving

    Args:
        input_file (str): Path to input CSV file
        output_dir (str): Directory to save processed files
        use_user_ids (bool): Whether to replace usernames with user IDs
    """
    # Load the CSV file
    df = pd.read_csv(input_file)

    # Load username to ID mapping if requested
    username_to_id = {}
    if use_user_ids:
        username_to_id = load_user_ids_mapping(input_file)

    # Apply transformations
    transformations = {
        'sqrt': apply_sqrt_transformation,
        'log': apply_log_transformation,
        'quantile': apply_quantile_transformation
    }

    base_name = Path(input_file).stem
    print(f"Processing {input_file}:")

    for transform_name, transform_func in transformations.items():
        # Apply transformation to all users
        users_transformed = transform_func(df.copy())

        # Replace usernames with user IDs in 'i' column if mapping is available and requested
        if use_user_ids and username_to_id:
            def replace_username_with_id(username):
                return username_to_id.get(username, username)

            users_transformed['i'] = users_transformed['i'].apply(replace_username_with_id)
            replaced_count = sum(1 for username in df['i'] if username in username_to_id)
            print(f"    Replaced {replaced_count}/{len(df)} usernames with user IDs")

        # Sort by score (descending)
        users_transformed = users_transformed.sort_values('v', ascending=False)

        # Generate output file name
        users_output = os.path.join(output_dir, f"{base_name}_users_{transform_name}.csv")

        # Save the processed file
        users_transformed.to_csv(users_output, index=False)

        # Show score ranges
        users_min = users_transformed['v'].min() if len(users_transformed) > 0 else 0
        users_max = users_transformed['v'].max() if len(users_transformed) > 0 else 0

        print(f"  - {transform_name.capitalize()} transformation:")
        print(f"    Users: {len(users_transformed)} entries -> {users_output}")
        print(f"    Score range: {users_min:.2f} - {users_max:.2f}")


def main():
    """
    Main function to process all score files
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process score files with transformations')
    parser.add_argument('--use-user-ids', action='store_true',
                       help='Replace usernames with user IDs from raw/user_ids_*.csv files')
    args = parser.parse_args()
    # Define directories
    scores_dir = "scores"
    output_dir = "output"

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Find all CSV files in the scores directory
    scores_path = Path(scores_dir)
    csv_files = list(scores_path.glob("*.csv"))

    if not csv_files:
        print(f"No CSV files found in {scores_dir} directory")
        return

    print(f"Found {len(csv_files)} score files to process...")
    print()

    # Process each CSV file
    for csv_file in csv_files:
        try:
            process_scores(str(csv_file), output_dir, args.use_user_ids)
            print()
        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")
            print()

    print("Processing complete!")


if __name__ == "__main__":
    main()
