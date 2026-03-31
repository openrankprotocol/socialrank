# Discord Social Rank Tools

A collection of Python scripts for Discord data collection, trust analysis, and social ranking.

## Scripts Overview

| Script | Description |
|--------|-------------|
| `read_messages.py` | Collects messages from Discord servers |
| `generate_trust.py` | Calculates trust values between users |
| `check_channels.py` | Shows all channels the bot has access to with permissions |
| `find_mods.py` | Finds moderators and administrators in Discord servers |
| `load_discord_data.py` | Loads collected Discord data into a PostgreSQL database |
| `process_scores.py` | Processes score files with transformations (log, sqrt, quantile) |
| `process_seed.py` | Processes seed score CSV files with tier-based weighting |
| `generate_json.py` | Generates JSON files for the UI from seed and score data |
| `summarize_posts.py` | Summarizes channel posts using OpenAI GPT |

## Setup

### 1. Virtual Environment

```bash
python3 -m venv venv
source ./venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

**Dependencies:**
- `discord.py` - Discord API wrapper
- `python-dotenv` - Environment variable management
- `toml` - Configuration file parsing
- `pandas` - Data processing
- `scipy` - Scientific computing (for score transformations)
- `openai` - OpenAI API (for post summarization)
- `psycopg2` - PostgreSQL adapter (for database operations)

### 3. Configure Environment

```bash
cp .env.example .env
# Edit .env and add your tokens
```

**Required environment variables:**
- `DISCORD_TOKEN` - Your Discord bot token
- `DISCORD_CLIENT_ID` - Your Discord application client ID (optional)
- `DATABASE_URL` - PostgreSQL connection string (for database scripts)
- `OPENAI_API_KEY` - OpenAI API key (for summarization)

### 4. Edit config.toml

```toml
[settings]
output_folder = "raw"
default_days = 730
max_messages_per_channel = 20000
channel_delay = 1
server_delay = 5

[exclusions]
exclude_channel_ids = [
    "channel_id_to_exclude",
]

[trust]
mention_points = 50
reply_points = 40
reaction_points = 30
here_points = 10
everyone_points = 10
role_points = 10

[servers]
server_ids = [
    "1021933844481445989",  # Your server ID
    "another_server_id",    # Add more servers
]
```

## Usage

### Step 1: Check Bot Access (Optional)

```bash
# Verify which channels the bot can access
python check_channels.py
```

### Step 2: Find Moderators (Optional)

```bash
# List all moderators and admins in servers
python find_mods.py
python find_mods.py --verbose  # Show detailed role information
```

### Step 3: Collect Discord Messages

```bash
# Process all servers from config.toml
python read_messages.py

# Override settings via command line
python read_messages.py --days 30 --max-messages 1000
```

### Step 4: Calculate Trust Values

```bash
# Calculate trust from collected messages
python generate_trust.py

# Use custom input path
python generate_trust.py --input raw
```

### Step 5: Load Data into Database (Optional)

```bash
# Load collected JSON data into PostgreSQL
python load_discord_data.py
```

### Step 6: Process Scores

```bash
# Process scores with log transformation (default)
python process_scores.py

# Use square root transformation
python process_scores.py --sqrt

# Use quantile transformation
python process_scores.py --quantile
```

### Step 7: Process Seed Scores

```bash
# Process seed CSV with default tier weights (60%, 20%, 20%)
python process_seed.py seed/my_seed.csv

# Custom tier weights
python process_seed.py seed/my_seed.csv --weights 0.5 0.3 0.2
```

### Step 8: Generate UI JSON

```bash
# Generate JSON files for the UI
python generate_json.py
```

### Step 9: Summarize Posts (Optional)

```bash
# Generate AI summaries for channel posts
python summarize_posts.py
```

## Directory Structure

```
socialrank/
├── raw/                    # Raw Discord message data (JSON)
├── trust/                  # Trust score CSVs per server
├── scores/                 # Score files for processing
├── output/                 # Processed score output
├── seed/                   # Seed data CSVs
├── ui/                     # Generated JSON for UI
├── schemas/                # SQL schema definitions
├── migrations/             # Database migrations
├── config.toml             # Main configuration file
├── requirements.txt        # Python dependencies
└── *.py                    # Python scripts
```

## Output Formats

### Discord Messages (`raw/`)

Messages saved as `ServerName.json` with structure:
```json
{
  "server_info": { "id": "...", "name": "..." },
  "channels": {
    "channel_id": {
      "channel_info": { "name": "..." },
      "messages": [...]
    }
  }
}
```

User ID mappings saved as `user_ids_ServerName.csv`.

### Trust Values (`trust/`)

Trust scores saved as CSV files per server (e.g., `karma3_labs.csv`):

```csv
i,j,v
user_id_1,user_id_2,150
user_id_1,user_id_3,80
```

Where:
- `i` = user ID giving trust
- `j` = user ID receiving trust
- `v` = trust value (sum of all interactions)

### Processed Scores (`output/`)

Transformed and normalized scores with usernames:

```csv
i,v
username1,1000.00
username2,850.50
```

### UI JSON (`ui/`)

Combined seed and score data for the UI:

```json
{
  "category": "socialrank",
  "server": "server_id",
  "seed": [{"i": "username", "v": 0.6}],
  "scores": [{"i": "username", "v": 950.0}]
}
```

## Trust Scoring System

| Interaction | Points |
|-------------|--------|
| Direct mention (@user) | 50 |
| Reply to message | 40 |
| React to message | 30 |
| Use @here | 10 (to everyone) |
| Use @everyone | 10 (to everyone) |
| Use @role | 10 (to role members) |

**Note:** Bots are excluded from giving or receiving trust values.

## Database Schema

The project includes PostgreSQL migrations in `migrations/` and schemas in `schemas/` for storing:
- Servers, channels, and users
- Messages with attachments, embeds, and reactions
- Mentions and role mentions
- Computed scores
- Channel summaries (AI-generated)

## Troubleshooting

### Bot Permissions

If the bot can't read messages:
1. Run `python check_channels.py` to see access status
2. Ensure the bot has "View Channel" and "Read Message History" permissions
3. Enable required intents in Discord Developer Portal

### Privileged Intents

For `find_mods.py`, enable "Server Members Intent" at:
https://discord.com/developers/applications/

### Database Connection

Ensure `DATABASE_URL` is set correctly:
```
postgresql://user:password@host:port/database
```
