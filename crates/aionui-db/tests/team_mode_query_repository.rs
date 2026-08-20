use aionui_db::models::TeamRow;
use aionui_db::{
    ITeamModeRepository, ITeamRepository, SqliteTeamModeRepository, SqliteTeamRepository, TeamGitDeliveryRow,
    TeamWorkItemRow, init_database_memory,
};

fn team(id: &str) -> TeamRow {
    TeamRow {
        id: id.to_owned(),
        user_id: "owner".into(),
        name: format!("Team {id}"),
        workspace: String::new(),
        workspace_mode: "shared".into(),
        agents: "[]".into(),
        lead_agent_id: None,
        session_mode: None,
        agents_version: "1.0.1".into(),
        created_at: 1,
        updated_at: 1,
    }
}

fn work_item(id: &str, team_id: &str, created_at: i64) -> TeamWorkItemRow {
    TeamWorkItemRow {
        id: id.to_owned(),
        team_id: team_id.to_owned(),
        parent_work_item_id: None,
        subject: format!("Work {id}"),
        description: None,
        controller_member_id: "lead".into(),
        assignee_member_id: "worker".into(),
        reviewer_member_id: "lead".into(),
        integrator_member_id: Some("lead".into()),
        delivery_requirement: "git".into(),
        git_repository_id: Some("repo".into()),
        git_base_commit: Some("base".into()),
        git_branch_ref: Some(format!("refs/heads/{id}")),
        state: "draft".into(),
        current_submission_json: None,
        accepted_delivery_json: None,
        revision: 0,
        created_at,
        updated_at: created_at,
    }
}

fn delivery(id: &str, team_id: &str, work_item_id: &str, content_revision: i64, updated_at: i64) -> TeamGitDeliveryRow {
    TeamGitDeliveryRow {
        id: id.to_owned(),
        team_id: team_id.to_owned(),
        work_item_id: work_item_id.to_owned(),
        producer_member_id: "worker".into(),
        repository_id: "repo".into(),
        content_revision,
        base_commit: "base".into(),
        branch_ref: format!("refs/heads/{id}"),
        head_commit: format!("head-{id}"),
        state: "submitted".into(),
        revision: 0,
        merged_commit: None,
        created_at: updated_at,
        updated_at,
    }
}

async fn insert_work_item(pool: &sqlx::SqlitePool, row: &TeamWorkItemRow) {
    sqlx::query(
        "INSERT INTO team_work_items \
         (id, team_id, parent_work_item_id, subject, description, controller_member_id, \
          assignee_member_id, reviewer_member_id, integrator_member_id, delivery_requirement, \
          git_repository_id, git_base_commit, git_branch_ref, state, current_submission_json, \
          accepted_delivery_json, revision, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&row.id)
    .bind(&row.team_id)
    .bind(&row.parent_work_item_id)
    .bind(&row.subject)
    .bind(&row.description)
    .bind(&row.controller_member_id)
    .bind(&row.assignee_member_id)
    .bind(&row.reviewer_member_id)
    .bind(&row.integrator_member_id)
    .bind(&row.delivery_requirement)
    .bind(&row.git_repository_id)
    .bind(&row.git_base_commit)
    .bind(&row.git_branch_ref)
    .bind(&row.state)
    .bind(&row.current_submission_json)
    .bind(&row.accepted_delivery_json)
    .bind(row.revision)
    .bind(row.created_at)
    .bind(row.updated_at)
    .execute(pool)
    .await
    .expect("insert WorkItem fixture");
}

async fn insert_delivery(pool: &sqlx::SqlitePool, row: &TeamGitDeliveryRow) {
    sqlx::query(
        "INSERT INTO team_git_deliveries \
         (id, team_id, work_item_id, producer_member_id, repository_id, content_revision, base_commit, \
          branch_ref, head_commit, state, revision, merged_commit, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&row.id)
    .bind(&row.team_id)
    .bind(&row.work_item_id)
    .bind(&row.producer_member_id)
    .bind(&row.repository_id)
    .bind(row.content_revision)
    .bind(&row.base_commit)
    .bind(&row.branch_ref)
    .bind(&row.head_commit)
    .bind(&row.state)
    .bind(row.revision)
    .bind(&row.merged_commit)
    .bind(row.created_at)
    .bind(row.updated_at)
    .execute(pool)
    .await
    .expect("insert delivery fixture");
}

#[tokio::test]
async fn list_queries_are_team_scoped_filtered_and_deterministic() {
    let database = init_database_memory().await.expect("initialize database");
    let team_repo = SqliteTeamRepository::new(database.pool().clone());
    team_repo.create_team(&team("team-a")).await.unwrap();
    team_repo.create_team(&team("team-b")).await.unwrap();

    for row in [
        work_item("work-later", "team-a", 20),
        work_item("work-first", "team-a", 10),
        work_item("work-other-team", "team-b", 5),
    ] {
        insert_work_item(database.pool(), &row).await;
    }
    for row in [
        delivery("delivery-middle", "team-a", "work-first", 1, 20),
        delivery("delivery-latest", "team-a", "work-later", 1, 30),
        delivery("delivery-oldest", "team-a", "work-first", 2, 10),
        delivery("delivery-other-team", "team-b", "work-other-team", 1, 40),
    ] {
        insert_delivery(database.pool(), &row).await;
    }

    let repository = SqliteTeamModeRepository::new(database.pool().clone());
    let work_item_ids = repository
        .list_work_items("team-a")
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();
    assert_eq!(work_item_ids, ["work-first", "work-later"]);

    let all_delivery_ids = repository
        .list_git_deliveries("team-a", None)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();
    assert_eq!(
        all_delivery_ids,
        ["delivery-latest", "delivery-middle", "delivery-oldest"]
    );

    let filtered_delivery_ids = repository
        .list_git_deliveries("team-a", Some("work-first"))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();
    assert_eq!(filtered_delivery_ids, ["delivery-middle", "delivery-oldest"]);
}
