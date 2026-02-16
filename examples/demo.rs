use oxidb::OxiDb;
use serde_json::json;

fn main() -> oxidb::Result<()> {
    let db = OxiDb::open(std::path::Path::new("./oxidb_data"))?;

    // Create indexes before inserting (or after — backfill works either way)
    db.create_index("events", "type")?;
    db.create_index("events", "created_at")?;
    db.create_composite_index("events", vec!["type".into(), "created_at".into()])?;

    // Insert documents
    db.insert_many(
        "events",
        vec![
            json!({"type": "click", "created_at": "2024-01-15T10:30:00Z", "user": "alice"}),
            json!({"type": "view",  "created_at": "2024-03-20T14:00:00Z", "user": "bob"}),
            json!({"type": "click", "created_at": "2024-06-01T09:00:00Z", "user": "charlie"}),
            json!({"type": "click", "created_at": "2024-08-10T16:45:00Z", "user": "alice"}),
            json!({"type": "view",  "created_at": "2025-01-05T12:00:00Z", "user": "bob"}),
        ],
    )?;

    println!("=== All click events ===");
    let clicks = db.find("events", &json!({"type": "click"}))?;
    for doc in &clicks {
        println!("  {}", doc);
    }

    println!("\n=== Events in H1 2024 (date range query) ===");
    let h1 = db.find(
        "events",
        &json!({
            "created_at": {"$gte": "2024-01-01", "$lt": "2024-07-01"}
        }),
    )?;
    for doc in &h1 {
        println!("  {}", doc);
    }

    println!("\n=== Click events after 2024-05-01 (composite) ===");
    let recent_clicks = db.find(
        "events",
        &json!({
            "type": "click",
            "created_at": {"$gte": "2024-05-01"}
        }),
    )?;
    for doc in &recent_clicks {
        println!("  {}", doc);
    }

    // Update
    let updated = db.update(
        "events",
        &json!({"user": "alice", "type": "click"}),
        &json!({"$set": {"reviewed": true}}),
    )?;
    println!("\n=== Updated {} documents ===", updated);

    // Delete
    let deleted = db.delete("events", &json!({"type": "view"}))?;
    println!("=== Deleted {} view events ===", deleted);

    println!("\n=== Remaining: {} events ===", db.find("events", &json!({}))?.len());

    Ok(())
}
