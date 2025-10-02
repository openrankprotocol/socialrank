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

        # Check channel name exclusions (case-insensitive)
        exclude_names = [name.lower() for name in exclusions.get('exclude_channel_names', [])]
        if channel.name.lower() in exclude_names:
            return True

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

        total_messages = 0
        successful_channels = 0
        skipped_channels = 0

        logger.info(f"Processing {len(guild.text_channels)} channels in {guild.name}")

        for i, channel in enumerate(guild.text_channels, 1):
            logger.info(f"\n--- Processing channel #{channel.name} ({i}/{len(guild.text_channels)}) ---")

            try:
                # Check if channel should be excluded
                if self.should_exclude_channel(channel):
                    logger.warning(f"Skipping #{channel.name} - excluded by configuration")
                    skipped_channels += 1
                    continue

                # Check permissions
                if not channel.permissions_for(guild.me).read_message_history:
                    logger.warning(f"Skipping #{channel.name} - no read message history permission")
                    skipped_channels += 1
                    continue

                if not channel.permissions_for(guild.me).view_channel:
                    logger.warning(f"Skipping #{channel.name} - no view channel permission")
                    skipped_channels += 1
                    continue

                # Read messages from this channel
                messages = await self.read_single_channel(channel, days_back, max_messages)

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
                if i < len(guild.text_channels):
                    await asyncio.sleep(self.config['settings'].get('channel_delay', 1))

            except Exception as e:
                logger.error(f"Error reading #{channel.name}: {str(e)}")
                skipped_channels += 1

        # Add summary to server data
        all_server_data['summary'] = {
            'total_messages': total_messages,
            'successful_channels': successful_channels,
            'skipped_channels': skipped_channels,
            'total_channels': len(guild.text_channels)
        }

        logger.info(f"\n🎉 {guild.name} summary: {total_messages} total messages from {successful_channels}/{len(guild.text_channels)} channels")

        return all_server_data

    async def read_single_channel(self, channel, days_back=7, max_messages=None):
        """
        Read messages from a single channel object
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
            messages_data = []
            message_count = 0

            async for message in channel.history(after=cutoff_date, limit=max_messages):
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
                    'reactions': [
                        {
                            'emoji': str(reaction.emoji),
                            'count': reaction.count
                        } for reaction in message.reactions
                    ],
                    'pinned': message.pinned,
                    'mention_everyone': message.mention_everyone,
                    'mentions': [str(user.id) for user in message.mentions],
                    'role_mentions': [str(role.id) for role in message.role_mentions],
                    'message_type': str(message.type),
                    'flags': message.flags.value if message.flags else 0
                }

                messages_data.append(message_data)
                message_count += 1

                # Log progress for large channels
                if message_count % 500 == 0:
                    logger.info(f"  ... {message_count} messages processed")

            return messages_data

        except Exception as e:
            logger.error(f"Error reading messages from #{channel.name}: {str(e)}")
            return []

    def save_server_data(self, server_data, guild, days_back):
        """Save server data to file"""
        try:
            filename = self.get_server_filename(guild, days_back)
            filepath = self.output_folder / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(server_data, f, indent=2, ensure_ascii=False)

            logger.info(f"💾 Server data saved to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error saving server data: {str(e)}")
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
