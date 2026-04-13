CREATE TABLE IF NOT EXISTS page_view_counts (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    page_id TEXT,
    view_count INT,
    PRIMARY KEY (window_start, window_end, page_id)
);

CREATE TABLE IF NOT EXISTS active_users (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    active_users INT,
    PRIMARY KEY (window_start, window_end)
);

CREATE TABLE IF NOT EXISTS user_sessions (
    user_id TEXT,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    duration INT,
    PRIMARY KEY (user_id, session_start)
);