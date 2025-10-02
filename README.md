# Discord Social Rank Tools

Two Python scripts for Discord data collection and trust analysis:
1. `read_messages.py` - Collects messages from Discord servers
2. `generate_trust.py` - Calculates trust values between users

## Setup

### 1. Virtual Environment
```bash
source ./d_env/bin/activate
```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env and add your Discord bot token
   ```

4. **Edit config.toml:**
   ```toml
   [servers]
   server_ids = [
       "1021933844481445989",  # Your server ID
       "another_server_id",    # Add more servers
   ]
   ```

## Usage

### Step 1: Collect Discord Messages

```bash
# Process all servers from config.toml
python read_messages.py

# Override settings
python read_messages.py --days 30 --max-messages 1000
```

### Step 2: Calculate Trust Values

```bash
# Calculate trust from collected messages
python generate_trust.py

# Use custom input/output paths
python generate_trust.py --input raw --output trust/trust_scores.csv
```

## Output

### Discord Messages
Messages saved to `./raw/` folder:
- `ServerName.json`

### Trust Values
Trust scores saved to `./trust/` folder with one CSV file per server:
- `karma3_labs.csv`
- `another_server.csv`

Each CSV file has format:
```csv
i,j,v
user_id_1,user_id_2,150
user_id_1,user_id_3,80
```

Where:
- `i` = user giving trust
- `j` = user receiving trust
- `v` = trust value (sum of all interactions)

## Trust Scoring System

- Direct mention (@user): **50 points**
- Reply to message: **40 points**
- React to message: **30 points**
- Use @here: **10 points** to everyone
- Use @everyone: **10 points** to everyone
- Use @role: **10 points** to role members

**Note:** Bots are excluded from giving or receiving trust values.
