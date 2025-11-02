import discord
from discord.ext import commands
import asyncio
import os
from datetime import datetime, timedelta, timezone
import json
import argparse
import logging
import toml
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DiscordChannelReader:
    def __init__(self, token, config):
        # Set up bot with necessary intents
        intents = discord.Intents.default()
        intents.message_content = True  # Required to read message content
        intents.guild_messages = True   # Required to read guild messages

        self.bot = commands.Bot(
            command_prefix='!',
            intents=intents,
            help_command=None
        )

        self.token = token
        self.config = config

        # Create output folder if it doesn't exist
        self.output_folder = Path(self.config['settings']['output_folder'])
        self.output_folder.mkdir(exist_ok=True)

        # Set up event handlers
        self.setup_events()

    def setup_events(self):
        @self.bot.event
        async def on_ready():
            logger.info(f'{self.bot.user} has connected to Discord!')
            logger.info(f'Bot is in {len(self.bot.guilds)} servers')
            for guild in self.bot.guilds:
                logger.info(f'  - {guild.name} (ID: {guild.id}, {len(guild.text_channels)} text channels)')

    def sanitize_filename(self, filename):
        """Sanitize server name for use as filename"""
        # Convert to lowercase and replace whitespace with underscores
        filename = filename.lower().replace(' ', '_')
        # Remove/replace problematic characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename.strip()

    def get_server_filename(self, guild, days_back):
        """Generate filename for server data"""
        sanitized_name = self.sanitize_filename(guild.name)
        return f"{sanitized_name}.json"

    def should_exclude_channel(self, channel):
        """Check if a channel should be excluded based on config settings"""
        exclusions = self.config.get('exclusions', {})

        # Check channel ID exclusions
        exclude_ids = exclusions.get('exclude_channel_ids', [])
        if str(channel.id) in exclude_ids:
            return True

        return False

    async def read_all_channels_in_server(self, guild, days_back=7, max_messages=None):
        """
        Read messages from all text channels in a server

        Args:
            guild: Discord guild object
            days_back (int): Number of days to look back
            max_messages (int): Max messages per channel

        Returns:
            dict: Dictionary with channel_id as key and messages as value
        """
        all_server_data = {
            'server_info': {
                'id': str(guild.id),
                'name': guild.name,
                'member_count': guild.member_count,
                'created_at': guild.created_at.isoformat(),
                'fetch_timestamp': datetime.now(timezone.utc).isoformat(),
                'days_back': days_back,
                'max_messages_per_channel': max_messages
            },
            'channels': {}
        }

        # Track unique users for username-to-ID mapping
        unique_users = {}

        total_messages = 0
        successful_channels = 0
        skipped_channels = 0

        # First, check channel access for all channels
        accessible_channels = []
        logger.info(f"Checking access to {len(guild.text_channels)} channels in {guild.name}")

        for channel in guild.text_channels:
            permissions = channel.permissions_for(guild.me)

            # Check key permissions for reading messages
            can_view = permissions.view_channel
            can_read_history = permissions.read_message_history

            if can_view:
                # Try channels with view permission, even without explicit read_message_history
                if not self.should_exclude_channel(channel):
                    accessible_channels.append(channel)
                    if can_read_history:
                        logger.debug(f"✅ #{channel.name} - ACCESSIBLE")
                    else:
                        logger.debug(f"⚠️ #{channel.name} - VIEW ONLY (will attempt)")
                else:
                    logger.debug(f"⚠️ #{channel.name} - EXCLUDED by configuration")
                    skipped_channels += 1
            else:
                logger.debug(f"❌ #{channel.name} - NO VIEW permission")
                skipped_channels += 1

        logger.info(f"Found {len(accessible_channels)}/{len(guild.text_channels)} accessible channels")

        if not accessible_channels:
            logger.warning(f"⚠️ No accessible channels found in {guild.name}")
            all_server_data['summary'] = {
                'total_messages': 0,
                'successful_channels': 0,
                'skipped_channels': len(guild.text_channels),
                'total_channels': len(guild.text_channels)
            }
            all_server_data['unique_users'] = unique_users
            return all_server_data

        # Process only accessible channels
        for i, channel in enumerate(accessible_channels, 1):
            logger.info(f"\n--- Processing channel #{channel.name} ({i}/{len(accessible_channels)}) ---")

            try:

                # Read messages from this channel
                messages = await self.read_single_channel(channel, days_back, max_messages, unique_users)

                # Store channel info and messages
                channel_data = {
                    'channel_info': {
                        'id': str(channel.id),
                        'name': channel.name,
                        'type': str(channel.type),
                        'category': channel.category.name if channel.category else None,
                        'position': channel.position,
                        'topic': channel.topic,
                        'created_at': channel.created_at.isoformat()
                    },
                    'messages': messages
                }

                all_server_data['channels'][str(channel.id)] = channel_data
                total_messages += len(messages)
                successful_channels += 1

                logger.info(f"✅ #{channel.name}: {len(messages)} messages")

                # Small delay between channels
                if i < len(accessible_channels):
                    await asyncio.sleep(self.config['settings'].get('channel_delay', 1))

            except Exception as e:
                permissions = channel.permissions_for(guild.me)
                has_read_history = permissions.read_message_history
                if not has_read_history:
                    logger.warning(f"❌ #{channel.name}: Cannot read messages (no read_message_history permission) - {str(e)}")
                else:
                    logger.error(f"❌ #{channel.name}: Unexpected error - {str(e)}")
                skipped_channels += 1

        # Add summary to server data
        all_server_data['summary'] = {
            'total_messages': total_messages,
            'successful_channels': successful_channels,
            'skipped_channels': skipped_channels,
            'total_channels': len(guild.text_channels),
            'accessible_channels': len(accessible_channels)
        }

        logger.info(f"\n🎉 {guild.name} summary: {total_messages} total messages from {successful_channels}/{len(accessible_channels)} accessible channels ({len(accessible_channels)}/{len(guild.text_channels)} total)")

        # Add unique users to server data
        all_server_data['unique_users'] = unique_users

        return all_server_data

    async def read_single_channel(self, channel, days_back=7, max_messages=None, unique_users=None):
        """
        Read messages from a single channel object
        """
        if unique_users is None:
            unique_users = {}

        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
            messages_data = []
            message_count = 0

            history_start = asyncio.get_event_loop().time()
            total_reaction_time = 0.0
            oldest_message_timestamp = None

            async for message in channel.history(after=cutoff_date, limit=max_messages):
                # Track author
                unique_users[str(message.author.id)] = message.author.name

                # Track mentioned users
                for user in message.mentions:
                    unique_users[str(user.id)] = user.name

                # Track reaction users with timing
                reaction_start = asyncio.get_event_loop().time()
                reactions_with_users = await self.get_reaction_users(message)
                reaction_end = asyncio.get_event_loop().time()
                total_reaction_time += (reaction_end - reaction_start)

                for reaction in reactions_with_users:
                    for user in reaction.get('users', []):
                        unique_users[str(user['id'])] = user['username']

                message_data = {
                    'id': str(message.id),
                    'author': {
                        'id': str(message.author.id),
                        'username': message.author.name,
                        'display_name': message.author.display_name,
                        'bot': message.author.bot,
                        'system': message.author.system if hasattr(message.author, 'system') else False
                    },
                    'content': message.content,
                    'timestamp': message.created_at.isoformat(),
                    'edited_timestamp': message.edited_at.isoformat() if message.edited_at else None,
                    'attachments': [
                        {
                            'id': str(att.id),
                            'filename': att.filename,
                            'url': att.url,
                            'size': att.size,
                            'content_type': att.content_type
                        } for att in message.attachments
                    ],
                    'embeds': [
                        {
                            'title': embed.title,
                            'description': embed.description,
                            'url': embed.url,
                            'color': embed.color.value if embed.color else None,
                            'timestamp': embed.timestamp.isoformat() if embed.timestamp else None
                        } for embed in message.embeds
                    ],
                    'reactions': reactions_with_users,
                    'pinned': message.pinned,
                    'mention_everyone': message.mention_everyone,
                    'mentions': [str(user.id) for user in message.mentions],
                    'role_mentions': [str(role.id) for role in message.role_mentions],
                    'message_type': str(message.type),
                    'flags': message.flags.value if message.flags else 0
                }

                messages_data.append(message_data)
                message_count += 1

                # Track oldest message timestamp
                if oldest_message_timestamp is None or message.created_at < oldest_message_timestamp:
                    oldest_message_timestamp = message.created_at

                # Log progress for large channels
                if message_count % 500 == 0 and message_count > 0:
                    elapsed = asyncio.get_event_loop().time() - history_start
                    avg_per_msg = elapsed / message_count
                    logger.info(f"     📊 Progress: {message_count} messages processed in {elapsed:.2f}s (avg {avg_per_msg:.3f}s/msg)")


            history_end = asyncio.get_event_loop().time()
            total_time = history_end - history_start
            if message_count > 0:
                logger.info(f"     ✅ Completed: {message_count} messages in {total_time:.2f}s")
                if total_reaction_time > 0.5:  # Only show if reactions took significant time
                    logger.info(f"     ⏰ Total reaction processing time: {total_reaction_time:.2f}s")
                if oldest_message_timestamp:
                    logger.info(f"     📅 Oldest message: {oldest_message_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            else:
                logger.info(f"     ⚪ No messages found in time range")


            return messages_data

        except Exception as e:
            logger.error(f"Error reading messages from #{channel.name}: {str(e)}")
            return []

    async def get_reaction_users(self, message):
        """Get users who reacted to a message"""
        reactions_data = []

        if not message.reactions:
            return reactions_data

        for reaction in message.reactions:
            try:
                # Get users who made this reaction
                users = []
                async for user in reaction.users():
                    users.append({
                        'id': str(user.id),
                        'username': user.name,
                        'bot': user.bot
                    })

                reactions_data.append({
                    'emoji': str(reaction.emoji),
                    'count': reaction.count,
                    'users': users
                })
            except Exception as e:
                # If we can't get reaction users, fall back to count only
                reactions_data.append({
                    'emoji': str(reaction.emoji),
                    'count': reaction.count,
                    'users': []
                })

        return reactions_data

    def save_server_data(self, server_data, guild, days_back):
        """Save server data to file"""
        try:
            filename = self.get_server_filename(guild, days_back)
            filepath = self.output_folder / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(server_data, f, indent=2, ensure_ascii=False)

            logger.info(f"💾 Server data saved to {filepath}")

            # Also save username-to-ID mapping
            self.save_user_ids_mapping(server_data, guild)

            return filepath
        except Exception as e:
            logger.error(f"Error saving server data: {str(e)}")
            return None

    def save_user_ids_mapping(self, server_data, guild):
        """Save username to user ID mapping CSV"""
        try:
            sanitized_name = self.sanitize_filename(guild.name)
            csv_filename = f"user_ids_{sanitized_name}.csv"
            csv_filepath = self.output_folder / csv_filename

            unique_users = server_data.get('unique_users', {})

            with open(csv_filepath, 'w', encoding='utf-8') as f:
                f.write('username,user_id\n')
                for user_id, username in unique_users.items():
                    # Escape commas in usernames by wrapping in quotes
                    if ',' in username:
                        username = f'"{username}"'
                    f.write(f'{username},{user_id}\n')

            logger.info(f"💾 User IDs mapping saved to {csv_filepath}")
            return csv_filepath
        except Exception as e:
            logger.error(f"Error saving user IDs mapping: {str(e)}")
            return None

    async def process_servers(self, server_ids=None, days_back=None, max_messages=None):
        """
        Process multiple servers from config
        """
        # Use config defaults if not specified
        if days_back is None:
            days_back = self.config['settings'].get('default_days', 7)
        if max_messages is None:
            max_messages = self.config['settings'].get('max_messages_per_channel', 0)
            if max_messages == 0:
                max_messages = None

        # Use server IDs from config if not specified
        if server_ids is None:
            server_ids = [int(sid) for sid in self.config['servers']['server_ids']]

        logger.info(f"Processing {len(server_ids)} servers with {days_back} days lookback")

        results = {}

        for i, server_id in enumerate(server_ids, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"PROCESSING SERVER {i}/{len(server_ids)}: {server_id}")
            logger.info(f"{'='*60}")

            try:
                # Find the guild
                guild = self.bot.get_guild(server_id)
                if not guild:
                    logger.error(f"❌ Server {server_id} not found or bot doesn't have access")
                    continue

                # Check server-specific overrides
                server_days = days_back
                server_max_messages = max_messages

                overrides = self.config['servers'].get('overrides', {})
                if str(server_id) in overrides:
                    override = overrides[str(server_id)]
                    server_days = override.get('days', days_back)
                    server_max_messages = override.get('max_messages', max_messages)
                    logger.info(f"Using server-specific settings: {server_days} days, {server_max_messages} max messages")

                # Process the server
                server_data = await self.read_all_channels_in_server(guild, server_days, server_max_messages)

                # Save to file
                filepath = self.save_server_data(server_data, guild, server_days)

                results[server_id] = {
                    'guild_name': guild.name,
                    'total_messages': server_data['summary']['total_messages'],
                    'successful_channels': server_data['summary']['successful_channels'],
                    'filepath': str(filepath) if filepath else None
                }

                # Delay between servers
                if i < len(server_ids):
                    delay = self.config['settings'].get('server_delay', 5)
                    logger.info(f"Waiting {delay} seconds before next server...")
                    await asyncio.sleep(delay)

            except Exception as e:
                logger.error(f"Error processing server {server_id}: {str(e)}")
                results[server_id] = {'error': str(e)}

        return results

    async def start_and_process(self, server_ids=None, days_back=None, max_messages=None):
        """
        Start the bot and process servers
        """
        try:
            # Start the bot
            logger.info("Logging in to Discord...")
            await self.bot.login(self.token)
            logger.info("✅ Login successful")

            logger.info("Starting bot...")
            connect_task = asyncio.create_task(self.bot.connect())
            await asyncio.sleep(3)  # Give it a moment to connect
            logger.info("✅ Bot connection started")

            # Process servers
            results = await self.process_servers(server_ids, days_back, max_messages)

            return results

        except discord.LoginFailure:
            logger.error("Invalid bot token provided")
            return None
        except Exception as e:
            logger.error(f"Error starting bot: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
        finally:
            if not self.bot.is_closed():
                logger.info("Closing bot connection...")
                await self.bot.close()

def load_config(config_path="config.toml"):
    """Load configuration from TOML file"""
    try:
        with open(config_path, 'r') as f:
            config = toml.load(f)
        logger.info(f"✅ Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"❌ Configuration file {config_path} not found")
        return None
    except Exception as e:
        logger.error(f"❌ Error loading configuration: {str(e)}")
        return None

async def main():
    """Main function to run the Discord channel reader"""
    parser = argparse.ArgumentParser(description='Read Discord messages from multiple servers')
    parser.add_argument('--config', default='config.toml', help='Configuration file path')
    parser.add_argument('--days', type=int, help='Override default days to look back')
    parser.add_argument('--max-messages', type=int, help='Override max messages per channel')
    parser.add_argument('--servers', nargs='+', help='Override server IDs to process')

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    if not config:
        return

    # Get token from environment variable
    token = os.getenv('DISCORD_TOKEN')
    if not token:
        logger.error("DISCORD_TOKEN not found in environment variables. Please set it in your .env file.")
        return

    # Parse server IDs if provided
    server_ids = None
    if args.servers:
        server_ids = [int(sid) for sid in args.servers]
        logger.info(f"Override: Processing specific servers: {server_ids}")

    # Create reader instance
    reader = DiscordChannelReader(token, config)

    # Process servers
    logger.info("🚀 Starting Discord Channel Reader...")
    start_time = datetime.now(timezone.utc)

    results = await reader.start_and_process(
        server_ids=server_ids,
        days_back=args.days,
        max_messages=args.max_messages
    )

    total_time = datetime.now(timezone.utc) - start_time

    # Print final summary
    if results:
        logger.info(f"\n{'='*60}")
        logger.info("FINAL SUMMARY")
        logger.info(f"{'='*60}")

        total_messages = 0
        successful_servers = 0

        for server_id, result in results.items():
            if 'error' in result:
                logger.info(f"❌ Server {server_id}: ERROR - {result['error']}")
            else:
                total_messages += result['total_messages']
                successful_servers += 1
                logger.info(f"✅ {result['guild_name']}: {result['total_messages']} messages from {result['successful_channels']} channels")
                logger.info(f"   📁 Saved to: {result['filepath']}")

        logger.info(f"\n🎉 COMPLETED: {total_messages} total messages from {successful_servers}/{len(results)} servers in {total_time.total_seconds():.1f} seconds")
        logger.info(f"📂 Files saved in: {reader.output_folder.absolute()}")
    else:
        logger.error("❌ Failed to process servers")

if __name__ == "__main__":
    asyncio.run(main())
