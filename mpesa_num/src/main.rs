use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use anyhow::Result;
use mpesa_yum::YumMutUnsafePointer;

#[get("/status")]
async fn get_status(data: web::Data<YumMutUnsafePointer>) -> impl Responder{
    let percentage = data.get().percentage;
    HttpResponse::Ok().body(format!("{percentage}%"))
}

#[get("/open/{hash}")]
async fn get_hash(info: web::Path<String>, data: web::Data<YumMutUnsafePointer>)-> impl Responder {
    let hash = info.into_inner();
    println!("GOT HASH {hash}");
    let result = match data.get().get(&hash) {
        Ok(result) => match result {
            Some(result) => HttpResponse::Ok().body(result),
            None => HttpResponse::NotFound().body("has not yet available, check /status end point for progress")
        } 
        _ => HttpResponse::NotFound().body("has not yet available, check /status end point for progress")
    };
    result
}

fn fill_db(mpesa: &mut mpesa_yum::Yum)->Result<()>{
    mpesa.start_filling()
}

#[actix_web::main]
async fn main()-> Result<()> {

    let mut mpesa = mpesa_yum::init(Some("./db"))?;
    println!("DB created at {}",mpesa.location);

    let (mpesa_clone, data) = unsafe {
        let ptr: *mut mpesa_yum::Yum = &mut mpesa;
        (&mut *ptr, YumMutUnsafePointer{ptr})
    };

    let _ = futures::join!(
        tokio::spawn(async move {
            fill_db(mpesa_clone).unwrap()
        }),
        async {
            let _ = HttpServer::new( move || {
                App::new()
                    .app_data(web::Data::new(data))
                    .service(get_status)
                    .service(get_hash)
            }).workers(2).bind(("0.0.0.0", 42069)).unwrap().run().await;
        }
    );


    Ok(())
}
