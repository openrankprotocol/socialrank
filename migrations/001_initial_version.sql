BEGIN;

CREATE SCHEMA socialrank AUTHORIZATION k3l_user;
GRANT USAGE ON SCHEMA socialrank TO k3l_readonly, "openrank-api";

SET search_path TO socialrank;

CREATE TABLE servers (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    member_count bigint NOT NULL,
    created_at timestamp with time zone NOT NULL,
    fetch_timestamp timestamp with time zone NOT NULL,
    days_back bigint NOT NULL,
    max_messages_per_channel bigint NOT NULL
);

ALTER TABLE servers OWNER TO k3l_user;
GRANT SELECT ON TABLE servers TO k3l_readonly, "openrank-api";

CREATE TABLE users (
    id bigint PRIMARY KEY,
    username text,
    display_name text,
    bot boolean,
    system boolean
);

ALTER TABLE users OWNER TO k3l_user;
GRANT SELECT ON TABLE users TO k3l_readonly, "openrank-api";

CREATE TABLE roles (
    id bigint PRIMARY KEY
);

ALTER TABLE roles OWNER TO k3l_user;
GRANT SELECT ON TABLE roles TO k3l_readonly, "openrank-api";

CREATE TYPE channel_type AS ENUM ('text', 'news');

ALTER TYPE channel_type OWNER TO k3l_user;
GRANT USAGE ON TYPE channel_type TO k3l_readonly, "openrank-api";

CREATE TABLE channels (
    id bigint PRIMARY KEY,
    server_id bigint REFERENCES servers (id),
    name text,
    type channel_type,
    category text,
    position bigint,
    topic text,
    created_at timestamp with time zone
);

ALTER TABLE channels OWNER TO k3l_user;
GRANT SELECT ON TABLE channels TO k3l_readonly, "openrank-api";

CREATE TYPE message_type AS ENUM (
    'MessageType.default',
    'MessageType.reply',
    'MessageType.chat_input_command',
    'MessageType.pins_add',
    'MessageType.new_member',
    'MessageType.premium_guild_subscription'
);

ALTER TYPE message_type OWNER TO k3l_user;
GRANT USAGE ON TYPE message_type TO k3l_readonly, "openrank-api";

CREATE TABLE messages (
    id bigint PRIMARY KEY,
    channel_id bigint NOT NULL REFERENCES channels (id),
    author_id bigint NOT NULL REFERENCES users (id),
    content text NOT NULL,
    timestamp timestamp with time zone NOT NULL,
    edited_timestamp timestamp with time zone,
    pinned boolean NOT NULL,
    mention_everyone boolean NOT NULL,
    message_type message_type NOT NULL,
    flags bigint NOT NULL
);

ALTER TABLE messages OWNER TO k3l_user;
GRANT SELECT ON TABLE messages TO k3l_readonly, "openrank-api";

CREATE TABLE attachments (
    id bigint PRIMARY KEY,
    message_id bigint NOT NULL REFERENCES messages (id),
    filename text NOT NULL,
    url text NOT NULL,
    size bigint NOT NULL,
    content_type text NOT NULL
);

ALTER TABLE attachments OWNER TO k3l_user;
GRANT SELECT ON TABLE attachments TO k3l_readonly, "openrank-api";

CREATE TABLE embeds (
    message_id bigint NOT NULL REFERENCES messages (id),
    title text,
    description text,
    url text,
    color bigint,
    timestamp timestamp with time zone
);

ALTER TABLE embeds OWNER TO k3l_user;
GRANT SELECT ON TABLE embeds TO k3l_readonly, "openrank-api";

CREATE TABLE reactions (
    message_id bigint NOT NULL REFERENCES messages (id),
    emoji text NOT NULL,
    count bigint NOT NULL,
    PRIMARY KEY (message_id, emoji)
);

ALTER TABLE reactions OWNER TO k3l_user;
GRANT SELECT ON TABLE reactions TO k3l_readonly, "openrank-api";

CREATE TABLE reaction_users (
    message_id bigint NOT NULL,
    emoji text NOT NULL,
    user_id bigint NOT NULL,
    PRIMARY KEY (message_id, emoji, user_id),
    FOREIGN KEY (message_id, emoji) REFERENCES reactions (message_id, emoji),
    FOREIGN KEY (user_id) REFERENCES users (id)
);

ALTER TABLE reaction_users OWNER TO k3l_user;
GRANT SELECT ON TABLE reaction_users TO k3l_readonly, "openrank-api";

CREATE TABLE mentions (
    message_id bigint NOT NULL REFERENCES messages (id),
    user_id bigint NOT NULL REFERENCES users (id),
    PRIMARY KEY (message_id, user_id)
);

ALTER TABLE mentions OWNER TO k3l_user;
GRANT SELECT ON TABLE mentions TO k3l_readonly, "openrank-api";

CREATE TABLE role_mentions (
    message_id bigint NOT NULL REFERENCES messages (id),
    role_id bigint NOT NULL REFERENCES roles (id),
    PRIMARY KEY (message_id, role_id)
);

ALTER TABLE role_mentions OWNER TO k3l_user;
GRANT SELECT ON TABLE role_mentions TO k3l_readonly, "openrank-api";

CREATE TABLE runs (
    run_id bigserial PRIMARY KEY,
    server_id bigint NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (server_id) REFERENCES servers (id) ON DELETE CASCADE
);

ALTER TABLE runs OWNER TO k3l_user;
GRANT SELECT ON runs TO k3l_readonly, "openrank-api";

CREATE INDEX idx_socialrank_runs_server_id ON runs (server_id);
CREATE INDEX idx_socialrank_runs_created_at ON runs (created_at DESC);

CREATE TABLE seeds (
    server_id bigint NOT NULL,
    run_id bigint NOT NULL,
    user_id bigint NOT NULL,
    score DOUBLE PRECISION,
    PRIMARY KEY (server_id, run_id, user_id),
    FOREIGN KEY (server_id) REFERENCES servers (id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE
);

ALTER TABLE seeds OWNER TO k3l_user;
GRANT SELECT ON seeds TO k3l_readonly, "openrank-api";

CREATE INDEX idx_socialrank_seeds_server_run ON seeds (server_id, run_id);
CREATE INDEX idx_socialrank_seeds_score ON seeds (score DESC);

-- Create scores table with user IDs and score values
CREATE TABLE scores (
    server_id bigint NOT NULL,
    run_id bigint NOT NULL,
    user_id bigint NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (server_id, run_id, user_id),
    FOREIGN KEY (server_id) REFERENCES servers (id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE
);

ALTER TABLE scores OWNER TO k3l_user;
GRANT SELECT ON scores TO k3l_readonly, "openrank-api";

CREATE INDEX idx_socialrank_scores_server_run ON scores (server_id, run_id);
CREATE INDEX idx_socialrank_scores_value ON scores (value DESC);

COMMIT;
