use sqlx::postgres::PgPoolOptions;
use dotenv::dotenv;
use std::env;
use std::sync::Arc;
use serde::{Deserialize};
use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use actix_web::{post, web, App, HttpServer, Responder, HttpResponse};

#[derive(Deserialize)]
struct MessageRequest {
    content: String,
}

fn mqtt_client() -> (AsyncClient, EventLoop) {
    let duration = core::time::Duration::from_secs(5);
    let mut mqttoptions = MqttOptions::new("actix-mqtt", "localhost", 1883);
    mqttoptions.set_credentials("remy", "TheSecretPassword");
    mqttoptions.set_keep_alive(duration);
    AsyncClient::new(mqttoptions, 10)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to Postgres");

    let (mqtt, mut eventloop) = mqtt_client();
    // Lancer l'eventloop dans une tâche asynchrone
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("MQTT eventloop error: {:?}", e);
                    break;
                }
            }
        }
    });

    let mqtt = Arc::new(mqtt);
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::from(mqtt.clone()))
            .service(publish_message)
    })
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}

#[post("/publish")]
async fn publish_message(
    pool: web::Data<sqlx::PgPool>,
    mqtt: web::Data<AsyncClient>,
    msg: web::Json<MessageRequest>,
) -> impl Responder {
    // Publier sur MQTT
    let payload = msg.content.clone();

    // Insérer dans la base
    if let Err(e) = sqlx::query("INSERT INTO messages (content) VALUES ($1)")
        .bind(&msg.content)
        .execute(pool.get_ref())
        .await
    {
        return HttpResponse::InternalServerError().body(format!("DB error: {}", e));
    }

    if let Err(e) = mqtt
        .publish("messages/secure/api", QoS::AtLeastOnce, false, payload.clone())
        .await
    {
        return HttpResponse::InternalServerError().body(format!("MQTT error: {}", e));
    }

    HttpResponse::Ok().body("Message published and saved")
}