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
    Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: false,
        locking: true,
        no_tx: false,
    }
    .run(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn migration_029_preserves_existing_mailbox_rows_as_unkeyed() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    run_migrations_through(&pool, 28).await;

    sqlx::query(
        "INSERT INTO mailbox \
            (id, team_id, to_agent_id, from_agent_id, type, content, read, created_at) \
         VALUES ('message-1', 'team-1', 'lead-1', 'user', 'message', 'hello', 0, 1)",
    )
    .execute(&pool)
    .await
    .unwrap();

    run_migrations_through(&pool, 29).await;

    let row: (String, Option<String>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT content, idempotency_scope, idempotency_key, request_fingerprint \
         FROM mailbox WHERE id = 'message-1'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row, ("hello".into(), None, None, None));
}
