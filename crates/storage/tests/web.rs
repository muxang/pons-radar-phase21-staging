use pons_storage::repositories::{TokenListQuery, WebRepository};

#[sqlx::test(migrations = "../../migrations")]
async fn production_read_models_are_bounded_and_empty_safe(pool: sqlx::PgPool) {
    let repository = WebRepository::new(pool.clone());
    let dashboard = repository.dashboard().await.unwrap();
    assert_eq!(dashboard["high_priority"].as_array().unwrap().len(), 0);
    assert_eq!(dashboard["live_feed"].as_array().unwrap().len(), 0);

    let page = repository
        .tokens(&TokenListQuery {
            sort: "launch_time",
            descending: true,
            limit: 25,
            ..TokenListQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(page["total"], 0);
    assert_eq!(page["limit"], 25);
    assert!(page["items"].as_array().unwrap().is_empty());

    let system = repository.system().await.unwrap();
    assert_eq!(system["postgres"], "HEALTHY");
    assert_eq!(system["tracked_curves"], 0);
    assert!(system["workers"].is_object());

    let token_id: uuid::Uuid = sqlx::query_scalar("INSERT INTO tokens(chain_id,address,launch_time,launch_block,launch_log_index,lifecycle)VALUES(4663,decode('1111111111111111111111111111111111111111','hex'),now(),1,0,'ACTIVE_CURVE')RETURNING id").fetch_one(&pool).await.unwrap();
    let detail = repository
        .token(&[0x11; 20])
        .await
        .unwrap()
        .expect("inserted token");
    assert_eq!(detail["id"], token_id.to_string());
    assert_eq!(detail["signal"], serde_json::Value::Null);
    let timeline = repository.timeline(token_id, None, 20).await.unwrap();
    assert_eq!(timeline["items"][0]["type"], "TOKEN_LAUNCHED");
    assert!(
        repository.smart_money(token_id, 20, 0).await.unwrap()["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        repository.research(token_id).await.unwrap()["history"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}
