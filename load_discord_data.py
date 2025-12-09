#!/usr/bin/env python3
"""
Load Discord JSON data into PostgreSQL database.

This script reads JSON files from the raw/ directory and loads them into
a PostgreSQL database using psycopg v3 with asyncio.
"""
import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set

import psycopg
from dotenv import load_dotenv

# Set up logger
logger = logging.getLogger(__name__)

# Batch size for bulk inserts
BATCH_SIZE = 1000


def batch_items(items: List[Any], batch_size: int = BATCH_SIZE):
    """Yield successive batches of items."""
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


# Define allowed fields for each JSON object type to catch extra fields
ALLOWED_FIELDS = {
    'server_info': {
        'id', 'name', 'member_count', 'created_at', 'fetch_timestamp',
        'days_back', 'max_messages_per_channel'
    },
    'channel_info': {
        'id', 'name', 'type', 'category', 'position', 'topic', 'created_at'
    },
    'author': {
        'id', 'username', 'display_name', 'bot', 'system'
    },
    'message': {
        'id', 'author', 'content', 'timestamp', 'edited_timestamp',
        'attachments', 'embeds', 'reactions', 'mentions', 'role_mentions',
        'pinned', 'mention_everyone', 'message_type', 'flags'
    },
    'attachment': {
        'id', 'filename', 'url', 'size', 'content_type'
    },
    'embed': {
        'title', 'description', 'url', 'color', 'timestamp'
    },
    'reaction': {
        'emoji', 'count', 'users'
    },
    'reaction_user': {
        'id', 'username', 'bot'
    }
}


def validate_fields(obj: Dict[str, Any], allowed_fields: Set[str], obj_type: str) -> None:
    """Validate that object only contains allowed fields."""
    actual_fields = set(obj.keys())
    extra_fields = actual_fields - allowed_fields
    if extra_fields:
        raise ValueError(
            f"Extra fields found in {obj_type}: {extra_fields}\n"
            f"Object: {json.dumps(obj, indent=2)}"
        )


def parse_timestamp(ts_str: str | None) -> datetime | None:
    """Parse ISO format timestamp string to datetime."""
    if ts_str is None:
        return None
    return datetime.fromisoformat(ts_str)


async def execute_query(
    conn: psycopg.AsyncConnection,
    query: str,
    params: tuple | None = None
) -> None:
    """Execute a query with optional debug logging."""
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"SQL: {query}")
        if params:
            logger.debug(f"Params: {params}")
    await conn.execute(query, params)


async def execute_many(
    cur: psycopg.AsyncCursor,
    query: str,
    params_list: list
) -> None:
    """Execute a query multiple times with optional debug logging."""
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"SQL (executemany, {len(params_list)} rows): {query}")
        if params_list and len(params_list) <= 5:
            logger.debug(f"Params sample: {params_list}")
    await cur.executemany(query, params_list)


async def load_json_file(file_path: Path, conn: psycopg.AsyncConnection) -> None:
    """Load a single JSON file into the database."""
    logger.info(f"Loading {file_path}...")

    with open(file_path) as f:
        data = json.load(f)

    # Extract server info
    server_info = data['server_info']
    validate_fields(server_info, ALLOWED_FIELDS['server_info'], 'server_info')

    server_id = int(server_info['id'])

    # Insert server
    await insert_server(conn, server_info)

    # Collect all entities for batch insertion
    users_by_id = {}
    all_user_ids = set()  # All user IDs referenced anywhere
    all_role_ids = set()  # All role IDs referenced anywhere
    channels_list = []
    messages_list = []
    attachments_list = []
    embeds_list = []
    reactions_list = []
    reaction_users_list = []
    mentions_list = []
    role_mentions_list = []

    channels_data = data['channels']

    # First pass: collect all data
    for channel_id, channel_data in channels_data.items():
        channel_info = channel_data['channel_info']
        validate_fields(channel_info, ALLOWED_FIELDS['channel_info'], 'channel_info')

        channels_list.append({
            'id': int(channel_info['id']),
            'server_id': server_id,
            'name': channel_info.get('name'),
            'type': channel_info.get('type'),
            'category': channel_info.get('category'),
            'position': int(channel_info['position']) if channel_info.get('position') is not None else None,
            'topic': channel_info.get('topic'),
            'created_at': parse_timestamp(channel_info.get('created_at'))
        })

        for message in channel_data['messages']:
            validate_fields(message, ALLOWED_FIELDS['message'], 'message')

            message_id = int(message['id'])
            author = message['author']
            author_id = int(author['id'])

            # Track all user IDs
            all_user_ids.add(author_id)

            # Collect author details (if we have them)
            if author_id not in users_by_id:
                validate_fields(author, ALLOWED_FIELDS['author'], 'author')
                users_by_id[author_id] = {
                    'id': author_id,
                    'username': author['username'],
                    'display_name': author.get('display_name'),
                    'bot': author.get('bot', False),
                    'system': author.get('system', False)
                }

            # Collect message
            messages_list.append({
                'id': message_id,
                'channel_id': int(channel_id),
                'author_id': author_id,
                'content': message['content'],
                'timestamp': parse_timestamp(message['timestamp']),
                'edited_timestamp': parse_timestamp(message.get('edited_timestamp')),
                'pinned': message['pinned'],
                'mention_everyone': message['mention_everyone'],
                'message_type': message['message_type'],
                'flags': int(message['flags'])
            })

            # Collect attachments
            for attachment in message.get('attachments', []):
                validate_fields(attachment, ALLOWED_FIELDS['attachment'], 'attachment')
                attachments_list.append({
                    'id': int(attachment['id']),
                    'message_id': message_id,
                    'filename': attachment['filename'],
                    'url': attachment['url'],
                    'size': int(attachment['size']),
                    'content_type': attachment['content_type']
                })

            # Collect embeds
            for embed in message.get('embeds', []):
                validate_fields(embed, ALLOWED_FIELDS['embed'], 'embed')
                embeds_list.append({
                    'message_id': message_id,
                    'title': embed.get('title'),
                    'description': embed.get('description'),
                    'url': embed.get('url'),
                    'color': int(embed['color']) if embed.get('color') is not None else None,
                    'timestamp': parse_timestamp(embed.get('timestamp'))
                })

            # Collect reactions
            for reaction in message.get('reactions', []):
                validate_fields(reaction, ALLOWED_FIELDS['reaction'], 'reaction')
                emoji = reaction['emoji']

                reactions_list.append({
                    'message_id': message_id,
                    'emoji': emoji,
                    'count': int(reaction['count'])
                })

                # Collect reaction users
                for user in reaction['users']:
                    user_id = int(user['id'])

                    # Track all user IDs
                    all_user_ids.add(user_id)

                    # Also add reaction users to users_by_id (if we have details)
                    if user_id not in users_by_id:
                        validate_fields(user, ALLOWED_FIELDS['reaction_user'], 'reaction_user')
                        users_by_id[user_id] = {
                            'id': user_id,
                            'username': user['username'],
                            'display_name': None,
                            'bot': user.get('bot', False),
                            'system': False
                        }

                    reaction_users_list.append({
                        'message_id': message_id,
                        'emoji': emoji,
                        'user_id': user_id
                    })

            # Collect mentions
            for mention_id in message.get('mentions', []):
                user_id = int(mention_id)
                all_user_ids.add(user_id)
                mentions_list.append({
                    'message_id': message_id,
                    'user_id': user_id
                })

            # Collect role mentions
            for role_mention_id in message.get('role_mentions', []):
                role_id = int(role_mention_id)
                all_role_ids.add(role_id)
                role_mentions_list.append({
                    'message_id': message_id,
                    'role_id': role_id
                })

    # Now batch insert everything
    # First: skeletal upserts to ensure all referenced IDs exist
    await batch_upsert_user_ids(conn, list(all_user_ids))
    await batch_upsert_role_ids(conn, list(all_role_ids))

    # Then: full upserts for entities where we have complete data
    await batch_insert_users(conn, list(users_by_id.values()))
    await batch_insert_channels(conn, channels_list)

    # Delete existing embeds for all messages in batch (embeds table has no PK)
    if messages_list:
        message_ids = [msg['id'] for msg in messages_list]
        await batch_delete_embeds(conn, message_ids)

    await batch_insert_messages(conn, messages_list)
    await batch_insert_attachments(conn, attachments_list)
    await batch_insert_embeds(conn, embeds_list)
    await batch_insert_reactions(conn, reactions_list)
    await batch_insert_reaction_users(conn, reaction_users_list)
    await batch_insert_mentions(conn, mentions_list)
    await batch_insert_role_mentions(conn, role_mentions_list)

    logger.info(f"✓ Loaded {file_path}")


async def batch_upsert_user_ids(conn: psycopg.AsyncConnection, user_ids: List[int]) -> None:
    """Skeletal upsert of user IDs to ensure they exist before being referenced."""
    if not user_ids:
        return

    query = """
        INSERT INTO socialrank.users (id)
        VALUES (%s)
        ON CONFLICT (id) DO NOTHING
    """

    total = 0
    for batch in batch_items(user_ids):
        async with conn.cursor() as cur:
            await execute_many(
                cur,
                query,
                [(user_id,) for user_id in batch]
            )
        total += len(batch)

    logger.info(f"  Ensured {total} user IDs exist")


async def batch_upsert_role_ids(conn: psycopg.AsyncConnection, role_ids: List[int]) -> None:
    """Skeletal upsert of role IDs to ensure they exist before being referenced."""
    if not role_ids:
        return

    query = """
        INSERT INTO socialrank.roles (id)
        VALUES (%s)
        ON CONFLICT (id) DO NOTHING
    """

    total = 0
    for batch in batch_items(role_ids):
        async with conn.cursor() as cur:
            await execute_many(
                cur,
                query,
                [(role_id,) for role_id in batch]
            )
        total += len(batch)

    logger.info(f"  Ensured {total} role IDs exist")


async def insert_server(conn: psycopg.AsyncConnection, server_info: Dict[str, Any]) -> None:
    """Insert or update server information."""
    query = """
        INSERT INTO socialrank.servers
            (id, name, member_count, created_at, fetch_timestamp, days_back, max_messages_per_channel)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            member_count = EXCLUDED.member_count,
            created_at = EXCLUDED.created_at,
            fetch_timestamp = EXCLUDED.fetch_timestamp,
            days_back = EXCLUDED.days_back,
            max_messages_per_channel = EXCLUDED.max_messages_per_channel
    """

    await execute_query(
        conn,
        query,
        (
            int(server_info['id']),
            server_info['name'],
            int(server_info['member_count']),
            parse_timestamp(server_info['created_at']),
            parse_timestamp(server_info['fetch_timestamp']),
            int(server_info['days_back']),
            int(server_info['max_messages_per_channel'])
        )
    )


async def batch_insert_users(conn: psycopg.AsyncConnection, users: List[Dict[str, Any]]) -> None:
    """Insert or update users in batches (with full details)."""
    if not users:
        return

    query = """
        INSERT INTO socialrank.users
            (id, username, display_name, bot, system)
        VALUES
            (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            username = EXCLUDED.username,
            display_name = EXCLUDED.display_name,
            bot = EXCLUDED.bot,
            system = EXCLUDED.system
    """

    total = 0
    for batch in batch_items(users):
        async with conn.cursor() as cur:
            await execute_many(
                cur,
                query,
                [
                    (
                        user['id'],
                        user['username'],
                        user['display_name'],
                        user['bot'],
                        user['system']
                    )
                    for user in batch
                ]
            )
        total += len(batch)

    logger.info(f"  Inserted/updated {total} users with details")


async def batch_insert_channels(conn: psycopg.AsyncConnection, channels: List[Dict[str, Any]]) -> None:
    """Insert or update channels in batches."""
    if not channels:
        return

    query = """
        INSERT INTO socialrank.channels
            (id, server_id, name, type, category, position, topic, created_at)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            server_id = EXCLUDED.server_id,
            name = EXCLUDED.name,
            type = EXCLUDED.type,
            category = EXCLUDED.category,
            position = EXCLUDED.position,
            topic = EXCLUDED.topic,
            created_at = EXCLUDED.created_at
    """

    total = 0
    for batch in batch_items(channels):
        async with conn.cursor() as cur:
            await execute_many(
                cur,
                query,
                [
                    (
                        ch['id'],
                        ch['server_id'],
                        ch['name'],
                        ch['type'],
                        ch['category'],
                        ch['position'],
                        ch['topic'],
                        ch['created_at']
                    )
                    for ch in batch
                ]
            )
        total += len(batch)

    logger.info(f"  Inserted/updated {total} channels")


async def batch_delete_embeds(conn: psycopg.AsyncConnection, message_ids: List[int]) -> None:
    """Delete embeds for messages in batches."""
    if not message_ids:
        return

    query = "DELETE FROM socialrank.embeds WHERE message_id = ANY(%s)"

    total = 0
    for batch in batch_items(message_ids):
        await execute_query(conn, query, (batch,))
        total += len(batch)

    logger.debug(f"  Deleted embeds for {total} messages")


async def batch_insert_messages(conn: psycopg.AsyncConnection, messages: List[Dict[str, Any]]) -> None:
    """Insert or update messages in batches."""
    if not messages:
        return

    query = """
        INSERT INTO socialrank.messages
            (id, channel_id, author_id, content, timestamp, edited_timestamp,
             pinned, mention_everyone, message_type, flags)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            channel_id = EXCLUDED.channel_id,
            author_id = EXCLUDED.author_id,
            content = EXCLUDED.content,
            timestamp = EXCLUDED.timestamp,
            edited_timestamp = EXCLUDED.edited_timestamp,
            pinned = EXCLUDED.pinned,
            mention_everyone = EXCLUDED.mention_everyone,
            message_type = EXCLUDED.message_type,
            flags = EXCLUDED.flags
    """

    total = 0
    for batch in batch_items(messages):
        async with conn.cursor() as cur:
            await execute_many(
                cur,
                query,
                [
                    (
                        msg['id'],
                        msg['channel_id'],
                        msg['author_id'],
                        msg['content'],
                        msg['timestamp'],
                        msg['edited_timestamp'],
                        msg['pinned'],
                        msg['mention_everyone'],
                        msg['message_type'],
                        msg['flags']
                    )
                    for msg in batch
                ]
            )
        total += len(batch)

    logger.info(f"  Inserted/updated {total} messages")


async def batch_insert_attachments(conn: psycopg.AsyncConnection, attachments: List[Dict[str, Any]]) -> None:
    """Insert or update attachments in batches."""
    if not attachments:
        return

    query = """
        INSERT INTO socialrank.attachments
            (id, message_id, filename, url, size, content_type)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            message_id = EXCLUDED.message_id,
            filename = EXCLUDED.filename,
            url = EXCLUDED.url,
            size = EXCLUDED.size,
            content_type = EXCLUDED.content_type
    """

    total = 0
    for batch in batch_items(attachments):
        async with conn.cursor() as cur:
            await execute_many(
                cur,
                query,
                [
                    (
                        att['id'],
                        att['message_id'],
                        att['filename'],
                        att['url'],
                        att['size'],
                        att['content_type']
                    )
                    for att in batch
                ]
            )
        total += len(batch)

    logger.info(f"  Inserted/updated {total} attachments")


async def batch_insert_embeds(conn: psycopg.AsyncConnection, embeds: List[Dict[str, Any]]) -> None:
    """Insert embeds in batches (no upsert, deleted before insertion)."""
    if not embeds:
        return

    query = """
        INSERT INTO socialrank.embeds
            (message_id, title, description, url, color, timestamp)
        VALUES
            (%s, %s, %s, %s, %s, %s)
    """

    total = 0
    for batch in batch_items(embeds):
        async with conn.cursor() as cur:
            await execute_many(
                cur,
                query,
                [
                    (
                        emb['message_id'],
                        emb['title'],
                        emb['description'],
                        emb['url'],
                        emb['color'],
                        emb['timestamp']
                    )
                    for emb in batch
                ]
            )
        total += len(batch)

    logger.info(f"  Inserted {total} embeds")


async def batch_insert_reactions(conn: psycopg.AsyncConnection, reactions: List[Dict[str, Any]]) -> None:
    """Insert or update reactions in batches."""
    if not reactions:
        return

    query = """
        INSERT INTO socialrank.reactions
            (message_id, emoji, count)
        VALUES
            (%s, %s, %s)
        ON CONFLICT (message_id, emoji) DO UPDATE SET
            count = EXCLUDED.count
    """

    total = 0
    for batch in batch_items(reactions):
        async with conn.cursor() as cur:
            await execute_many(
                cur,
                query,
                [
                    (
                        rxn['message_id'],
                        rxn['emoji'],
                        rxn['count']
                    )
                    for rxn in batch
                ]
            )
        total += len(batch)

    logger.info(f"  Inserted/updated {total} reactions")


async def batch_insert_reaction_users(conn: psycopg.AsyncConnection, reaction_users: List[Dict[str, Any]]) -> None:
    """Insert reaction users in batches."""
    if not reaction_users:
        return

    query = """
        INSERT INTO socialrank.reaction_users
            (message_id, emoji, user_id)
        VALUES
            (%s, %s, %s)
        ON CONFLICT (message_id, emoji, user_id) DO NOTHING
    """

    total = 0
    for batch in batch_items(reaction_users):
        async with conn.cursor() as cur:
            await execute_many(
                cur,
                query,
                [
                    (
                        ru['message_id'],
                        ru['emoji'],
                        ru['user_id']
                    )
                    for ru in batch
                ]
            )
        total += len(batch)

    logger.info(f"  Inserted {total} reaction users")


async def batch_insert_mentions(conn: psycopg.AsyncConnection, mentions: List[Dict[str, Any]]) -> None:
    """Insert mentions in batches."""
    if not mentions:
        return

    query = """
        INSERT INTO socialrank.mentions
            (message_id, user_id)
        VALUES
            (%s, %s)
        ON CONFLICT (message_id, user_id) DO NOTHING
    """

    total = 0
    for batch in batch_items(mentions):
        async with conn.cursor() as cur:
            await execute_many(
                cur,
                query,
                [
                    (
                        m['message_id'],
                        m['user_id']
                    )
                    for m in batch
                ]
            )
        total += len(batch)

    logger.info(f"  Inserted {total} mentions")


async def batch_insert_role_mentions(conn: psycopg.AsyncConnection, role_mentions: List[Dict[str, Any]]) -> None:
    """Insert role mentions in batches."""
    if not role_mentions:
        return

    query = """
        INSERT INTO socialrank.role_mentions
            (message_id, role_id)
        VALUES
            (%s, %s)
        ON CONFLICT (message_id, role_id) DO NOTHING
    """

    total = 0
    for batch in batch_items(role_mentions):
        async with conn.cursor() as cur:
            await execute_many(
                cur,
                query,
                [
                    (
                        rm['message_id'],
                        rm['role_id']
                    )
                    for rm in batch
                ]
            )
        total += len(batch)

    logger.info(f"  Inserted {total} role mentions")


async def main() -> None:
    """Main entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Load Discord JSON data into PostgreSQL database'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug output (logs all SQL queries)'
    )
    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Load environment variables
    load_dotenv()

    # Get database connection parameters from environment
    db_params = {
        'host': os.getenv('PGHOST'),
        'port': int(os.getenv('PGPORT', '5432')),
        'user': os.getenv('PGUSER'),
        'dbname': os.getenv('PGDATABASE')
    }

    # Password is optional if using .pgpass file
    password = os.getenv('PGPASSWORD')
    if password:
        db_params['password'] = password

    # Validate required parameters (password is optional)
    required_params = ['host', 'user', 'dbname']
    missing = [k for k in required_params if db_params.get(k) is None]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(f'PG{k.upper()}' for k in missing)}")
        sys.exit(1)

    # Find all JSON files in raw/
    raw_dir = Path('raw')
    if not raw_dir.exists():
        logger.error(f"Directory {raw_dir} does not exist")
        sys.exit(1)

    json_files = list(raw_dir.glob('*.json'))
    if not json_files:
        logger.error(f"No JSON files found in {raw_dir}")
        sys.exit(1)

    logger.info(f"Found {len(json_files)} JSON file(s) to process")

    # Connect to database and process files
    async with await psycopg.AsyncConnection.connect(**db_params) as conn:
        logger.info(f"Connected to database: {db_params['dbname']}")

        for json_file in json_files:
            try:
                async with conn.transaction():
                    await load_json_file(json_file, conn)
            except Exception as e:
                logger.error(f"Error loading {json_file}: {e}")
                raise

    logger.info("✓ All files loaded successfully!")


if __name__ == '__main__':
    asyncio.run(main())
