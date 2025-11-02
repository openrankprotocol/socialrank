#!/usr/bin/env python3
"""
Discord Server Moderators Finder

This script connects to Discord and finds all moderators and administrators
in the servers the bot has access to.

Usage:
    python3 find_mods.py

Requirements:
    - discord.py (install with: pip install discord.py)
    - python-dotenv (install with: pip install python-dotenv)
    - .env file with DISCORD_TOKEN and DISCORD_CLIENT_ID

Output:
    - Lists all servers the bot is in
    - Shows administrators and moderators for each server
    - Displays user IDs, usernames, and their roles
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

class ModeratorFinder:
    def __init__(self, token, client_id=None, verbose=False):
        # Set up bot with necessary intents
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True  # Needed to get member info

        self.bot = commands.Bot(
            command_prefix='!',
            intents=intents,
            help_command=None
        )

        self.token = token
        self.client_id = client_id
        self.verbose = verbose
        self.setup_events()

    def setup_events(self):
        @self.bot.event
        async def on_ready():
            logger.info(f'✅ Bot connected as {self.bot.user}')
            await self.find_all_moderators()
            await self.bot.close()

    def has_mod_permissions(self, member):
        """Check if a member has admin or mod roles for Ritual server"""
        # Get all role names for this member
        role_names = [role.name.lower() for role in member.roles]

        # Check for admin role
        if 'admin' in role_names:
            return "Administrator"

        # Check for mod role
        if 'mods' in role_names:
            return "Moderator"

        return None

    async def find_all_moderators(self):
        """Find all moderators in all accessible servers"""
        print(f"\n{'='*60}")
        print(f"DISCORD SERVER MODERATORS REPORT")
        print(f"Bot: {self.bot.user.name}#{self.bot.user.discriminator}")
        print(f"{'='*60}")

        total_servers = len(self.bot.guilds)
        total_mods = 0
        total_admins = 0

        for i, guild in enumerate(self.bot.guilds, 1):
            print(f"\n📊 SERVER {i}/{total_servers}: {guild.name}")
            print(f"Server ID: {guild.id}")
            print(f"Owner: {guild.owner.name}#{guild.owner.discriminator} (ID: {guild.owner.id})")
            print(f"Total Members: {guild.member_count}")
            print("-" * 50)

            # Find server owner
            print(f"👑 SERVER OWNER:")
            print(f"   {guild.owner.name}#{guild.owner.discriminator} (ID: {guild.owner.id})")

            # Find administrators and moderators
            admins = []
            mods = []

            try:

                member_count = 0

                async for member in guild.fetch_members(limit=None):
                    member_count += 1



                    if member.bot:  # Skip bots
                        continue

                    mod_level = self.has_mod_permissions(member)
                    if mod_level == "Administrator":
                        admins.append(member)
                    elif mod_level == "Moderator":
                        mods.append(member)



            except discord.Forbidden:
                print("❌ Cannot fetch members - missing permissions or privileged intents")
                print("   Enable 'Server Members Intent' in Discord Developer Portal")
                continue
            except asyncio.TimeoutError:
                print("❌ Timeout while fetching members - server too large or slow connection")
                continue
            except Exception as e:
                print(f"❌ Error fetching members: {str(e)}")
                continue

            # Display administrators
            if admins:
                print(f"\n👮 ADMINISTRATORS ({len(admins)}):")
                for admin in admins:
                    if self.verbose:
                        # Get their roles (excluding @everyone)
                        roles = [role.name for role in admin.roles if role.name != "@everyone"]
                        roles_str = ", ".join(roles) if roles else "No special roles"
                        print(f"   {admin.name}#{admin.discriminator}")
                        print(f"     └─ ID: {admin.id}")
                        print(f"     └─ Roles: {roles_str}")
                    else:
                        bot_indicator = " [BOT]" if admin.bot else ""
                        print(f"   {admin.name}#{admin.id}{bot_indicator}")
                    total_admins += 1
            else:
                print(f"\n👮 ADMINISTRATORS: None found")

            # Display moderators
            if mods:
                print(f"\n🛡️  MODERATORS ({len(mods)}):")
                for mod in mods:
                    if self.verbose:
                        # Get their roles (excluding @everyone)
                        roles = [role.name for role in mod.roles if role.name != "@everyone"]
                        roles_str = ", ".join(roles) if roles else "No special roles"
                        print(f"   {mod.name}#{mod.discriminator}")
                        print(f"     └─ ID: {mod.id}")
                        print(f"     └─ Roles: {roles_str}")
                    else:
                        bot_indicator = " [BOT]" if mod.bot else ""
                        print(f"   {mod.name}#{mod.id}{bot_indicator}")
                    total_mods += 1
            else:
                print(f"\n🛡️  MODERATORS: None found")

        # Summary
        print(f"\n{'='*60}")
        print(f"SUMMARY")
        print(f"{'='*60}")
        print(f"Total Servers: {total_servers}")
        print(f"Total Administrators: {total_admins}")
        print(f"Total Moderators: {total_mods}")
        print(f"Total Staff: {total_admins + total_mods}")

        if total_admins + total_mods == 0:
            print("\n⚠️  WARNING: No moderators or administrators found!")
            print("   This could be due to:")
            print("   - Missing 'Server Members Intent' (privileged)")
            print("   - Bot doesn't have permission to see member list")
            print("   - Server has very restrictive permissions")
            print("   - No users have moderator permissions")

    async def start_and_find(self):
        """Start the bot and find moderators"""
        try:
            await self.bot.start(self.token)
        except discord.LoginFailure:
            logger.error("❌ Invalid bot token")
        except discord.PrivilegedIntentsRequired as e:
            logger.error(f"❌ Privileged intents required: {e}")
            logger.error("   Enable 'Server Members Intent' at: https://discord.com/developers/applications/")
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")

def load_env_vars():
    """Load environment variables from .env file"""
    try:
        # Load .env file
        load_dotenv()

        token = os.getenv('DISCORD_TOKEN')
        client_id = os.getenv('DISCORD_CLIENT_ID')

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
    import argparse

    parser = argparse.ArgumentParser(description='Find Discord server moderators and administrators')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed information including roles and IDs')
    args = parser.parse_args()

    print("🛡️  Discord Server Moderators Finder")
    print("====================================")

    # Load environment variables
    token, client_id = load_env_vars()
    if not token:
        print("❌ Could not load DISCORD_TOKEN from .env file")
        print("   Create a .env file with: DISCORD_TOKEN=your_token_here")
        return

    if client_id:
        print(f"Client ID: {client_id}")

    print("\n⚠️  Note: This requires 'Server Members Intent' to be enabled")
    print("   Enable it at: https://discord.com/developers/applications/")

    # Initialize and run finder
    finder = ModeratorFinder(token, client_id, args.verbose)
    await finder.start_and_find()

if __name__ == "__main__":
    asyncio.run(main())
