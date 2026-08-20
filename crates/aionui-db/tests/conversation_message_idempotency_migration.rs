use std::borrow::Cow;
use std::path::Path;

use sqlx::migrate::Migrator;
use sqlx::sqlite::SqlitePoolOptions;

async fn run_migrations_through(pool: &sqlx::SqlitePool, max_version: i64) {
    let full = Migrator::new(Path::new("migrations")).await.unwrap();
    let migrations = full
        .migrations
        .iter()
        .filter(|migration| migration.version <= max_version)
        .cloned()
        .collect::<Vec<_>>();
    let migrator = Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: false,
        locking: true,
        no_tx: false,
    };
    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF; PRAGMA legacy_alter_table = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    migrator.run(&mut *connection).await.unwrap();
    sqlx::query("PRAGMA foreign_keys = ON; PRAGMA legacy_alter_table = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
}

#[tokio::test]
async fn migration_033_preserves_existing_messages_and_adds_receipt_schema() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    run_migrations_through(&pool, 32).await;

    sqlx::query(
        "INSERT INTO users (id, username, password_hash, created_at, updated_at) \
         VALUES ('system_default_user', 'system', '', 1, 1)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO conversations \
            (id, user_id, name, type, extra, pinned, created_at, updated_at) \
         VALUES ('conv-1', 'system_default_user', 'Conversation', 'acp', '{}', 0, 1, 1)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO messages \
            (id, conversation_id, msg_id, type, content, position, status, hidden, created_at) \
         VALUES ('msg-1', 'conv-1', 'msg-1', 'text', '{}', 'right', 'finish', 0, 1)",
    )
    .execute(&pool)
    .await
    .unwrap();

    run_migrations_through(&pool, 33).await;

    let message_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE id = 'msg-1'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(message_count, 1);
    let columns: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('conversation_message_receipts')")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        columns,
        vec![
            "conversation_id",
            "idempotency_key",
            "request_fingerprint",
            "message_id",
            "turn_id",
            "created_at"
        ]
    );
}
