#!/usr/bin/env python3
"""
Score Processing Script

This script processes score files from the scores/ directory by:
1. Loading all CSV score files
2. Treating all peers as Discord users (usernames only)
3. Applying transformations to make exponential distributions more linear
4. Normalizing scores so all scores sum to 1
5. Saving results to output/ directory as [channel_name].csv

Transformations available:
- Logarithmic (default): log transformation (first scaled to 1-100 range) to linearize exponential data
- Square root (--sqrt): square root transformation
- Quantile (--quantile): quantile-based uniform distribution transformation

Usage:
    python3 process_scores.py               # Uses log transformation (default)
    python3 process_scores.py --sqrt        # Uses square root transformation
    python3 process_scores.py --quantile    # Uses quantile transformation

Requirements:
    - pandas (install with: pip install pandas)
    - numpy (install with: pip install numpy)
    - scipy (for quantile transformation)
    - CSV files in scores/ directory with columns 'i' (identifier) and 'v' (score)

Output:
    - Creates output/ directory if it doesn't exist
    - For each input file (e.g., ai.csv), creates:
      - output/ai.csv: Processed scores with selected transformation
    - Scores are normalized and mapped to 0-1000 range, sorted by score (descending)
"""

import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats


def apply_sqrt_transformation(df):
    """Apply square root transformation to scores"""
    if len(df) == 0:
        return df

    df_transformed = df.copy()

    # Apply sqrt transformation
    df_transformed["v"] = np.sqrt(df["v"])

    # Normalize to 0-1 range
    min_val = df_transformed["v"].min()
    max_val = df_transformed["v"].max()
    if max_val != min_val:
        df_transformed["v"] = (df_transformed["v"] - min_val) / (max_val - min_val)
    else:
        df_transformed["v"] = 1.0 / len(df)

    # Map to 100-1000 range
    df_transformed["v"] = df_transformed["v"] * 1000

    # Round to 2 decimal places
    df_transformed["v"] = df_transformed["v"].round(2)

    return df_transformed


def apply_log_transformation(df):
    """Apply logarithmic transformation to scores (first scale to 1-100, then log)"""
    if len(df) == 0:
        return df

    df_transformed = df.copy()

    # First normalize to 0-1 range
    min_val = df["v"].min()
    max_val = df["v"].max()
    if max_val != min_val:
        df_transformed["v"] = (df["v"] - min_val) / (max_val - min_val)
    else:
        df_transformed["v"] = 1.0 / len(df)

    # Map to 1-100 range
    df_transformed["v"] = df_transformed["v"] * 99 + 1

    # Apply log transformation
    df_transformed["v"] = np.log(df_transformed["v"])

    # Normalize back to 0-1 range
    min_log = df_transformed["v"].min()
    max_log = df_transformed["v"].max()
    if max_log != min_log:
        df_transformed["v"] = (df_transformed["v"] - min_log) / (max_log - min_log)
    else:
        df_transformed["v"] = 1.0 / len(df)

    # Map to 100-1000 range
    df_transformed["v"] = df_transformed["v"] * 1000

    # Round to 2 decimal places
    df_transformed["v"] = df_transformed["v"].round(2)

    return df_transformed


def apply_quantile_transformation(df):
    """Apply quantile-based uniform distribution transformation"""
    if len(df) == 0:
        return df

    df_transformed = df.copy()

    # Use scipy for quantile transformation
    df_transformed["v"] = stats.rankdata(df["v"]) / len(df["v"])

    # Map to 100-1000 range
    df_transformed["v"] = df_transformed["v"] * 1000

    # Round to 2 decimal places
    df_transformed["v"] = df_transformed["v"].round(2)

    return df_transformed


def load_user_ids_mapping(scores_file):
    """
    Load user ID to username mapping from raw/user_ids_[channel_name].csv

    Args:
        scores_file (str): Path to the scores CSV file

    Returns:
        dict: Mapping of user_id to username, or empty dict if file not found
    """
    try:
        # Extract channel name from scores file
        scores_path = Path(scores_file)
        channel_name = scores_path.stem

        # Load the corresponding user_ids mapping file
        mapping_file = Path(f"raw/user_ids_{channel_name}.csv")

        if not mapping_file.exists():
            print(f"    Warning: {mapping_file} not found")
            return {}

        print(f"    Loading username mapping from: {mapping_file}")

        # Load as dataframe
        mapping_df = pd.read_csv(mapping_file)

        # Create dictionary mapping user_id -> username
        id_to_username = dict(
            zip(mapping_df["user_id"].astype(str), mapping_df["username"])
        )

        print(f"    Loaded {len(id_to_username)} user ID mappings")
        return id_to_username

    except Exception as e:
        print(f"    Warning: Could not load username mapping: {str(e)}")
        return {}


def process_scores(input_file, output_dir, transform_func, transform_name):
    """
    Process a single score file by applying transformation and saving

    Args:
        input_file (str): Path to input CSV file
        output_dir (str): Directory to save processed files
        transform_func (callable): Transformation function to apply
        transform_name (str): Name of the transformation
    """
    # Load the CSV file
    df = pd.read_csv(input_file)

    # Load user ID to username mapping
    id_to_username = load_user_ids_mapping(input_file)

    base_name = Path(input_file).stem
    print(f"Processing {input_file} with {transform_name} transformation:")

    # Apply transformation
    users_transformed = transform_func(df.copy())

    # Convert user IDs to usernames
    if id_to_username:
        users_transformed["i"] = (
            users_transformed["i"]
            .astype(str)
            .map(id_to_username)
            .fillna(users_transformed["i"])
        )
        replaced_count = sum(
            1 for user_id in df["i"].astype(str) if user_id in id_to_username
        )
        print(f"    Converted {replaced_count}/{len(df)} user IDs to usernames")

    # Sort by score (descending)
    users_transformed = users_transformed.sort_values("v", ascending=False)

    # Generate output file name (just channel_name.csv)
    output_file = os.path.join(output_dir, f"{base_name}.csv")

    # Save the processed file
    users_transformed.to_csv(output_file, index=False)

    # Show score ranges
    users_min = users_transformed["v"].min() if len(users_transformed) > 0 else 0
    users_max = users_transformed["v"].max() if len(users_transformed) > 0 else 0

    print(f"    Output: {output_file}")
    print(f"    Entries: {len(users_transformed)}")
    print(f"    Score range: {users_min:.2f} - {users_max:.2f}")


def main():
    """
    Main function to process all score files
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Process score files with transformations (default: log)"
    )

    # Create mutually exclusive group for transformation options
    transform_group = parser.add_mutually_exclusive_group()
    transform_group.add_argument(
        "--sqrt", action="store_true", help="Use square root transformation"
    )
    transform_group.add_argument(
        "--quantile", action="store_true", help="Use quantile transformation"
    )

    args = parser.parse_args()

    # Determine which transformation to use
    if args.sqrt:
        transform_func = apply_sqrt_transformation
        transform_name = "sqrt"
    elif args.quantile:
        transform_func = apply_quantile_transformation
        transform_name = "quantile"
    else:
        # Default to log transformation
        transform_func = apply_log_transformation
        transform_name = "log"

    print(f"Using {transform_name} transformation")
    print()

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
            process_scores(str(csv_file), output_dir, transform_func, transform_name)
            print()
        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")
            print()

    print("Processing complete!")


if __name__ == "__main__":
    main()
