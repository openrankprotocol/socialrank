#!/usr/bin/env python3
"""
JSON Generation Script for UI

This script generates JSON files for the UI by combining:
1. Seed data from seed/ directory (with user ID to username conversion)
2. Processed scores from output/ directory
3. Server information from raw/ directory

The output format is:
{
    "category": "socialrank",
    "server": "server_id",
    "seed": [{"i": "username", "v": score}, ...],
    "scores": [{"i": "username", "v": score}, ...]
}

Usage:
    python3 generate_json.py

Requirements:
    - pandas (install with: pip install pandas)
    - CSV files in seed/ directory
    - CSV files in output/ directory
    - JSON files in raw/ directory (for server ID mapping)

Output:
    - Creates ui/ directory if it doesn't exist
    - For each channel, creates ui/[channel_name].json
"""

import json
import os
from pathlib import Path

import pandas as pd


def load_server_id(channel_name):
    """
    Load server ID from raw/[channel_name].json

    Args:
        channel_name (str): Name of the channel

    Returns:
        str: Server ID, or None if not found
    """
    try:
        raw_file = Path(f"raw/{channel_name}.json")

        if not raw_file.exists():
            print(f"    Warning: {raw_file} not found")
            return None

        with open(raw_file, "r") as f:
            data = json.load(f)
            server_id = data.get("server_info", {}).get("id")

        if server_id:
            print(f"    Found server ID: {server_id}")
        else:
            print(f"    Warning: No server ID found in {raw_file}")

        return server_id
    except Exception as e:
        print(f"    Warning: Could not load server ID: {str(e)}")
        return None


def load_user_ids_mapping(channel_name):
    """
    Load user ID to username mapping from raw/user_ids_[channel_name].csv

    Args:
        channel_name (str): Name of the channel

    Returns:
        dict: Mapping of user_id to username, or empty dict if file not found
    """
    try:
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


def load_csv_to_dict_list(csv_path, id_to_username=None):
    """
    Load a CSV file and convert to list of dictionaries

    Args:
        csv_path (Path): Path to CSV file
        id_to_username (dict): Optional mapping of user IDs to usernames

    Returns:
        list: List of dictionaries with keys 'i' and 'v'
    """
    if not csv_path.exists():
        return []

    # Read CSV with dtype specified to prevent scientific notation
    df = pd.read_csv(csv_path, dtype={"i": str})

    # Convert user IDs to usernames if mapping is provided
    if id_to_username:
        df["i"] = df["i"].astype(str).map(id_to_username).fillna(df["i"])

    # Convert to list of dictionaries
    result = []
    for _, row in df.iterrows():
        result.append({"i": str(row["i"]), "v": float(row["v"])})

    return result


def generate_json(channel_name, ui_dir):
    """
    Generate JSON file for a specific channel

    Args:
        channel_name (str): Name of the channel
        ui_dir (str): Directory to save JSON files
    """
    print(f"Processing {channel_name}:")

    # Load server ID from raw data
    server_id = load_server_id(channel_name)

    # Load user ID to username mapping
    id_to_username = load_user_ids_mapping(channel_name)

    # Load seed data (with username conversion)
    seed_path = Path(f"seed/{channel_name}.csv")
    seed_data = load_csv_to_dict_list(seed_path, id_to_username)
    print(f"    Loaded {len(seed_data)} seed entries")

    # Load scores data (already has usernames from process_scores.py)
    scores_path = Path(f"output/{channel_name}.csv")
    scores_data = load_csv_to_dict_list(scores_path, None)
    print(f"    Loaded {len(scores_data)} score entries")

    # Create JSON structure
    json_data = {
        "category": "socialrank",
        "server": server_id if server_id else "",
        "seed": seed_data,
        "scores": scores_data,
    }

    # Generate output file path
    output_path = Path(ui_dir) / f"{channel_name}.json"

    # Save JSON file
    with open(output_path, "w") as f:
        json.dump(json_data, f, indent=2)

    print(f"    Saved to: {output_path}")


def main():
    """
    Main function to generate all JSON files
    """
    # Define directories
    seed_dir = "seed"
    output_dir = "output"
    ui_dir = "ui"

    # Ensure ui directory exists
    os.makedirs(ui_dir, exist_ok=True)

    # Find all CSV files in the seed directory
    seed_path = Path(seed_dir)
    seed_files = list(seed_path.glob("*.csv"))

    if not seed_files:
        print(f"No CSV files found in {seed_dir} directory")
        return

    print(f"Found {len(seed_files)} channels to process...")
    print()

    # Process each channel
    for seed_file in seed_files:
        channel_name = seed_file.stem

        try:
            generate_json(channel_name, ui_dir)
            print()
        except Exception as e:
            print(f"Error processing {channel_name}: {str(e)}")
            print()

    print("JSON generation complete!")


if __name__ == "__main__":
    main()
