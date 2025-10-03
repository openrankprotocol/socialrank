#!/usr/bin/env python3
"""
Trust Calculator for Discord Messages

Calculates trust values between users based on their interactions:
- Mentions: 50 points
- Replies: 40 points
- Reactions: 30 points
- @here: 10 points to everyone
- @everyone: 10 points to everyone
- @role: 10 points to everyone with that role
"""

import json
import logging
import argparse
import toml
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Set, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrustCalculator:
    def __init__(self, config):
        self.config = config
        self.trust_scores = defaultdict(lambda: defaultdict(int))  # trust_scores[from_user][to_user] = score
        self.user_info = {}  # Store user information
        self.server_members = {}  # Store server member lists

        # Hardcoded trust folder
        self.trust_folder = Path("trust")
        self.trust_folder.mkdir(exist_ok=True)

        # Trust score values
        self.scores = {
            'mention': 50,
            'reply': 40,
            'reaction': 30,
            'here': 10,
            'everyone': 10,
            'role': 10
        }

    def sanitize_server_name(self, server_name: str) -> str:
        """Sanitize server name for use as filename"""
        # Convert to lowercase, replace spaces with underscores, remove special chars
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9\s]', '', server_name)
        sanitized = sanitized.lower().replace(' ', '_')
        # Remove multiple underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        return sanitized.strip('_')

    def get_trust_csv_filename(self, server_name: str) -> str:
        """Generate CSV filename from server name"""
        sanitized_name = self.sanitize_server_name(server_name)
        return f"{sanitized_name}.csv"

    def load_server_data(self, filepath: Path) -> Dict[str, Any]:
        """Load server data from JSON file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data
        except Exception as e:
            return None

    def extract_user_info(self, server_data: Dict[str, Any]):
        """Extract user information from server data"""
        server_id = server_data['server_info']['id']
        server_name = server_data['server_info']['name']

        # Track all users mentioned in messages
        for channel_id, channel_data in server_data['channels'].items():
            for message in channel_data['messages']:
                author_id = message['author']['id']

                # Store author info
                self.user_info[author_id] = {
                    'username': message['author']['username'],
                    'display_name': message['author']['display_name'],
                    'bot': message['author']['bot'],
                    'servers': self.user_info.get(author_id, {}).get('servers', set()) | {server_id}
                }

                # Add to server members
                if server_id not in self.server_members:
                    self.server_members[server_id] = set()
                self.server_members[server_id].add(author_id)

                # Track mentioned users
                for mentioned_user_id in message['mentions']:
                    if mentioned_user_id not in self.user_info:
                        self.user_info[mentioned_user_id] = {
                            'username': f'user_{mentioned_user_id}',
                            'display_name': f'User {mentioned_user_id}',
                            'bot': False,
                            'servers': {server_id}
                        }
                    self.server_members[server_id].add(mentioned_user_id)

    def process_server_file(self, filepath: Path):
        """Process a single server file and calculate trust scores"""
        data = self.load_server_data(filepath)
        if not data:
            return None

        server_id = data['server_info']['id']
        server_name = data['server_info']['name']

        # Extract user information first
        self.extract_user_info(data)

        # Create server-specific trust scores
        server_trust_scores = defaultdict(lambda: defaultdict(int))
        total_messages = 0

        # Process each channel
        for channel_id, channel_data in data['channels'].items():
            channel_name = channel_data['channel_info']['name']
            messages = channel_data['messages']

            # Process each message
            for message in messages:
                author_id = message['author']['id']

                # Skip bot messages - bots cannot give trust
                if message['author']['bot']:
                    continue

                total_messages += 1

                # 1. Process direct mentions (50 points each)
                for mentioned_user_id in message['mentions']:
                    mentioned_user_is_bot = self.user_info.get(mentioned_user_id, {}).get('bot', False)
                    if mentioned_user_id != author_id and not mentioned_user_is_bot:
                        server_trust_scores[author_id][mentioned_user_id] += self.scores['mention']

                # 2. Process @everyone (10 points to everyone on server)
                if message['mention_everyone']:
                    server_members = self.server_members.get(server_id, set())
                    for member_id in server_members:
                        member_is_bot = self.user_info.get(member_id, {}).get('bot', False)
                        if member_id != author_id and not member_is_bot:
                            server_trust_scores[author_id][member_id] += self.scores['everyone']

                # 3. Process @here (check message content)
                content = message['content'].lower()
                if '@here' in content:
                    server_members = self.server_members.get(server_id, set())
                    for member_id in server_members:
                        member_is_bot = self.user_info.get(member_id, {}).get('bot', False)
                        if member_id != author_id and not member_is_bot:
                            server_trust_scores[author_id][member_id] += self.scores['here']

                # 4. Process role mentions (10 points to everyone with that role)
                for role_id in message['role_mentions']:
                    server_members = self.server_members.get(server_id, set())
                    for member_id in server_members:
                        member_is_bot = self.user_info.get(member_id, {}).get('bot', False)
                        if member_id != author_id and not member_is_bot:
                            server_trust_scores[author_id][member_id] += self.scores['role']

                # 5. Process reactions (30 points each)
                for reaction in message['reactions']:
                    for reaction_user in reaction.get('users', []):
                        reactor_id = reaction_user['id']
                        # Skip if reactor is bot or same as message author
                        if not reaction_user['bot'] and reactor_id != author_id:
                            server_trust_scores[reactor_id][author_id] += self.scores['reaction']

        # Save server-specific trust data
        output_file = self.save_trust_csv_for_server(server_name, server_trust_scores)

        # Calculate stats
        total_relationships = sum(len(trust_dict) for trust_dict in server_trust_scores.values())
        total_points = sum(sum(trust_dict.values()) for trust_dict in server_trust_scores.values())

        return {
            'server_name': server_name,
            'output_file': output_file,
            'total_messages': total_messages,
            'trust_relationships': total_relationships,
            'total_points': total_points
        }

    def save_trust_csv_for_server(self, server_name: str, server_trust_data):
        """Save trust matrix for a specific server to CSV file with i,j,v format"""
        try:
            filename = self.get_trust_csv_filename(server_name)
            output_file = self.trust_folder / filename

            csv_lines = ['i,j,v']
            relationship_count = 0

            for giver_id, trust_dict in server_trust_data.items():
                giver_username = self.user_info.get(giver_id, {}).get('username', f'user_{giver_id}')
                for receiver_id, trust_value in trust_dict.items():
                    receiver_username = self.user_info.get(receiver_id, {}).get('username', f'user_{receiver_id}')
                    csv_lines.append(f"{giver_username},{receiver_username},{trust_value}")
                    relationship_count += 1

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(csv_lines))

            return output_file
        except Exception as e:
            return None

    def process_all_files(self, raw_folder: Path):
        """Process all JSON files in the raw folder"""
        json_files = list(raw_folder.glob("*.json"))

        if not json_files:
            return []

        results = []
        for filepath in json_files:
            result = self.process_server_file(filepath)
            if result:
                results.append(result)

        return results

def load_config(config_path="config.toml"):
    """Load configuration from TOML file"""
    try:
        with open(config_path, 'r') as f:
            config = toml.load(f)
        return config
    except FileNotFoundError:
        return None
    except Exception as e:
        return None

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Calculate trust values from Discord messages')
    parser.add_argument('--config', default='config.toml', help='Configuration file path')
    parser.add_argument('--input', help='Input folder path (overrides config)')

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    if not config:
        return

    # Determine input folder
    if args.input:
        raw_folder = Path(args.input)
    else:
        raw_folder = Path(config['settings']['output_folder'])

    if not raw_folder.exists():
        return

    # Initialize calculator
    calculator = TrustCalculator(config)

    # Process all files
    results = calculator.process_all_files(raw_folder)

if __name__ == "__main__":
    main()
