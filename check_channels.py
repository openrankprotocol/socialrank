#!/usr/bin/env python3
"""
Discord Channel Access Checker

This script connects to Discord and shows all channels the bot has access to,
along with the specific permissions for each channel.

Usage:
    python3 check_channels.py

Requirements:
    - discord.py (install with: pip install discord.py)
    - python-dotenv (install with: pip install python-dotenv)
    - .env file with DISCORD_TOKEN and DISCORD_CLIENT_ID

Output:
    - Lists all servers the bot is in
    - Shows all channels in each server
    - Displays permissions for each channel
    - Highlights channels the bot can read messages from
"""

import asyncio
import discord
from discord.ext import commands
import os
from dotenv import load_dotenv
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ChannelChecker:
    def __init__(self, token, client_id=None):
        # Set up bot with minimal intents
        intents = discord.Intents.default()
        intents.guilds = True
        intents.guild_messages = True

        self.bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

        self.token = token
        self.client_id = client_id
        self.setup_events()

    def setup_events(self):
        @self.bot.event
        async def on_ready():
            logger.info(f"✅ Bot connected as {self.bot.user}")
            await self.check_all_channels()
            await self.bot.close()

    async def check_all_channels(self):
        """Check all channels the bot has access to"""
        print(f"\n{'=' * 60}")
        print(f"DISCORD BOT CHANNEL ACCESS REPORT")
        print(f"Bot: {self.bot.user.name}#{self.bot.user.discriminator}")
        print(f"{'=' * 60}")

        total_servers = len(self.bot.guilds)
        total_channels = 0
        accessible_channels = 0

        for i, guild in enumerate(self.bot.guilds, 1):
            print(f"\n📊 SERVER {i}/{total_servers}: {guild.name}")
            print(f"Server ID: {guild.id}")
            print(f"Members: {guild.member_count}")
            print(f"Text Channels: {len(guild.text_channels)}")
            print("-" * 50)

            if not guild.text_channels:
                print("❌ No text channels found or accessible")
                continue

            for j, channel in enumerate(guild.text_channels, 1):
                total_channels += 1
                permissions = channel.permissions_for(guild.me)

                # Check key permissions
                can_view = permissions.view_channel
                can_read_history = permissions.read_message_history
                can_send = permissions.send_messages

                # Get channel creation time
                created_at = channel.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")

                # Status indicator
                if can_view and can_read_history:
                    status = "✅ ACCESSIBLE"
                    accessible_channels += 1
                elif can_view:
                    status = "⚠️  VIEW ONLY"
                else:
                    status = "❌ NO ACCESS"

                print(f"  {j:2d}. #{channel.name:<20} (ID: {channel.id}) {status}")
                print(f"      Created: {created_at}")

        # Summary
        print(f"\n{'=' * 60}")
        print(f"SUMMARY")
        print(f"{'=' * 60}")
        print(f"Total Servers: {total_servers}")
        print(f"Total Text Channels: {total_channels}")
        print(f"Accessible Channels: {accessible_channels}")
        print(
            f"Access Rate: {(accessible_channels / total_channels * 100):.1f}%"
            if total_channels > 0
            else "Access Rate: 0%"
        )

        if accessible_channels == 0:
            print("\n⚠️  WARNING: Bot cannot read messages from any channels!")
            print("   This could be due to:")
            print("   - Missing 'View Channel' permission")
            print("   - Missing 'Read Message History' permission")
            print("   - Bot not added to any servers")
            print("   - Servers have restricted bot permissions")

    async def start_and_check(self):
        """Start the bot and perform the channel check"""
        try:
            await self.bot.start(self.token)
        except discord.LoginFailure:
            logger.error("❌ Invalid bot token")
        except discord.PrivilegedIntentsRequired as e:
            logger.error(f"❌ Privileged intents required: {e}")
            logger.error(
                "   Enable intents at: https://discord.com/developers/applications/"
            )
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")


def load_env_vars():
    """Load environment variables from .env file"""
    try:
        # Load .env file
        load_dotenv()

        token = os.getenv("DISCORD_TOKEN")
        client_id = os.getenv("DISCORD_CLIENT_ID")

        if not token:
            logger.error("❌ DISCORD_TOKEN not found in .env file")
            return None, None

        if not client_id:
            logger.warning("⚠️ DISCORD_CLIENT_ID not found in .env file (optional)")

        return token, client_id
    except Exception as e:
        logger.error(f"❌ Error loading .env file: {str(e)}")
        return None, None


async def main():
    """Main function"""
    print("🤖 Discord Bot Channel Access Checker")
    print("=====================================")

    # Load environment variables
    token, client_id = load_env_vars()
    if not token:
        print("❌ Could not load DISCORD_TOKEN from .env file")
        print("   Create a .env file with: DISCORD_TOKEN=your_token_here")
        return

    if client_id:
        print(f"Client ID: {client_id}")

    # Initialize and run checker
    checker = ChannelChecker(token, client_id)
    await checker.start_and_check()


if __name__ == "__main__":
    asyncio.run(main())
