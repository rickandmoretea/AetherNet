use std::sync::Arc;
use actix_web::{web, HttpResponse, Responder, HttpServer, App};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

#[derive(Deserialize)]
pub struct TimeRange {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct TrafficData {
    country: Option<String>,
    as_number: Option<i32>,
    traffic: i64,
}

pub async fn top_countries(
    db: web::Data<Arc<Client>>,
    time_range: web::Query<TimeRange>,
) -> impl Responder {
    let result = db.query(
        "SELECT src_country as country, SUM(bytes) as traffic
         FROM flow_data
         WHERE time >= $1 AND time <= $2
         GROUP BY src_country
         ORDER BY traffic DESC
         LIMIT 10",
        &[&time_range.start, &time_range.end],
    ).await;

    match result {
        Ok(rows) => {
            let data: Vec<TrafficData> = rows
                .iter()
                .map(|row| TrafficData {
                    country: row.get(0),
                    as_number: None,
                    traffic: row.get(1),
                })
                .collect();
            HttpResponse::Ok().json(data)
        }
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn top_as(
    db: web::Data<Arc<Client>>,
    time_range: web::Query<TimeRange>,
) -> impl Responder {
    let result = db.query(
        "SELECT src_as as as_number, SUM(bytes) as traffic
         FROM flow_data
         WHERE time >= $1 AND time <= $2
         GROUP BY src_as
         ORDER BY traffic DESC
         LIMIT 10",
        &[&time_range.start, &time_range.end],
    ).await;

    match result {
        Ok(rows) => {
            let data: Vec<TrafficData> = rows
                .iter()
                .map(|row| TrafficData {
                    country: None,
                    as_number: row.get(0),
                    traffic: row.get(1),
                })
                .collect();
            HttpResponse::Ok().json(data)
        }
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn traffic_by_country(
    db: web::Data<Arc<Client>>,
    time_range: web::Query<TimeRange>,
) -> impl Responder {
    let result = db.query(
        "SELECT src_country as country, SUM(bytes) as traffic
         FROM flow_data
         WHERE time >= $1 AND time <= $2
         GROUP BY src_country
         ORDER BY traffic DESC",
        &[&time_range.start, &time_range.end],
    ).await;

    match result {
        Ok(rows) => {
            let data: Vec<TrafficData> = rows
                .iter()
                .map(|row| TrafficData {
                    country: row.get(0),
                    as_number: None,
                    traffic: row.get(1),
                })
                .collect();
            HttpResponse::Ok().json(data)
        }
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn traffic_by_as(
    db: web::Data<Arc<Client>>,
    time_range: web::Query<TimeRange>,
) -> impl Responder {
    let result = db.query(
        "SELECT src_as as as_number, SUM(bytes) as traffic
         FROM flow_data
         WHERE time >= $1 AND time <= $2
         GROUP BY src_as
         ORDER BY traffic DESC",
        &[&time_range.start, &time_range.end],
    ).await;

    match result {
        Ok(rows) => {
            let data: Vec<TrafficData> = rows
                .iter()
                .map(|row| TrafficData {
                    country: None,
                    as_number: row.get(0),
                    traffic: row.get(1),
                })
                .collect();
            HttpResponse::Ok().json(data)
        }
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn run_api(db: Arc<Client>) -> std::io::Result<()> {
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(Arc::clone(&db)))
            .route("/top_countries", web::get().to(top_countries))
            .route("/top_as", web::get().to(top_as))
            .route("/traffic_by_country", web::get().to(traffic_by_country))
            .route("/traffic_by_as", web::get().to(traffic_by_as))
    })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}

