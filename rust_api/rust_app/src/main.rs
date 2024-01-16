use aws_sdk_dynamodb::{Client};
use aws_sdk_dynamodb::types::AttributeValue;

use lambda_http::{run, service_fn, Body, Error, Request, Response};
use serde::{Deserialize, Serialize};
use tracing::info;

use std::sync::Arc;
use std::collections::HashMap;

use aws_smithy_types::Blob;

use aws_sdk_firehose::operation::put_record::PutRecordInput;
use aws_sdk_firehose::types::Record;
use aws_sdk_firehose::Client as FirehoseClient;


// struct that handles the request for all methods
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Item {
    pub user_id: i64,
    pub username: String,
}

// specified struct that includes item + user_count
#[derive(Serialize)]
struct ResponseBody {
    item: Item,
    user_count: usize,
}

// main put request handler for inserting into dynamodb and retrieve user count
async fn putRequest(db_client: &Client, event: Request) -> Result<Response<Body>, Error> {
    
    // extract info from request
    let body = event.body();
    let s = std::str::from_utf8(body).expect("invalid utf-8 sequence");
    
    // cloudwatch log not neccesary
    info!(payload = %s, "JSON Payload received");

    // serialze json into struct
    let item = match serde_json::from_str::<Item>(s) {
        Ok(item) => item,
        Err(err) => {
            let resp = Response::builder()
                .status(400)
                .header("content-type", "text/html")
                .body(err.to_string().into())
                .map_err(Box::new)?;
            return Ok(resp);
        }
    };


    // insert item into dynamodb
    add_item(db_client, item.clone(), "myTable2").await?;

    // get user count from dynamodb
    let user_count = get_user_count(db_client, "myTable2".to_string()).await?;


    // create a struct to hold the item and user count
    let response_body = ResponseBody {
        item: item,
        user_count: user_count,
    };


    // deserialize into json to return response

    //let j = serde_json::to_string(&item)?; for disable scan
    let j = serde_json::to_string(&response_body)?;

    //Send back a 200 - success
    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/html")
        .body(j.into())
        .map_err(Box::new)?;
    Ok(resp)
}

// function that called by putRequest to insert operation
pub async fn add_item(client: &Client, item: Item, table: &str) -> Result<(), Error> {

    let user_id = aws_sdk_dynamodb::types::AttributeValue::N(item.user_id.to_string());
    let username = aws_sdk_dynamodb::types::AttributeValue::S(item.username);

    let request = client // add item to struct
        .put_item()
        .table_name(table)
        .item("user_id", user_id)
        .item("username", username);

    info!("adding item to DynamoDB");

    let _resp = request.send().await?; //send item to dynamodb

    Ok(())
}

// function that sends data to stream
async fn send_to_firehose(firehose_client: &FirehoseClient, username: String, firehose_name: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {    
    let config = aws_config::load_from_env().await; // gets config from environment.
    let firehose_client = aws_sdk_firehose::Client::new(&config);  // creates the Firehose client.

   // append  newline char to username
    let usernameWithNline = format!("{}\n", username);

    let username_bytes = Blob::new(usernameWithNline.into_bytes());
    let record = Record::builder().data(username_bytes).build()?;

    let input = PutRecordInput::builder()
        .delivery_stream_name(firehose_name.clone())
        .record(record.clone())
        .build()?;

    
    firehose_client.put_record() // sends data to firehose
        .delivery_stream_name(firehose_name)
        .record(record)
        .send()
        .await?;

    Ok(())
}

// main get request handler for getting username from dynamodb
async fn get_request(db_client: &Client, event: Request) -> Result<Response<Body>, Error> {
    
    let path = event.uri().path(); // extract user_id from request URI
    let path_parts: Vec<&str> = path.split('/').collect();
    if path_parts.len() < 3 {
        return Err(Error::from("Missing user_id in path"));
    }
    let user_id: i64 = path_parts[2].parse().map_err(|_| Error::from("Invalid user_id"))?;

    // get username for given user_id
    let username = get_item(&db_client, user_id).await?;

    // return username 
    Ok(Response::new(Body::from(username)))
}

// function that called by get_request to get username
async fn get_item(db_client: &Client, user_id: i64) -> Result<String, Error> {
    
    let config = aws_config::load_from_env().await; // gets config from environment.
    let firehose_client = aws_sdk_firehose::Client::new(&config);

    let response = db_client // extract username from client
        .get_item()
        .table_name("myTable2")
        .key("user_id", aws_sdk_dynamodb::types::AttributeValue::N(user_id.to_string()))
        .send()
        .await?;

    let username = response // extract username from the response
        .item
        .and_then(|item| {
            item.get("username").and_then(|attr| match attr {
                aws_sdk_dynamodb::types::AttributeValue::S(s) => Some(s.clone()),
                _ => None,
            })
        })
        .ok_or_else(|| Error::from("User not found"))?;
    
    // sends username to firehose
    let firehose_result = send_to_firehose(&firehose_client, username.clone(), "myStream2".to_string()).await; 

    if let Err(e) = firehose_result { // checks if firehose error
        println!("Failed to send data to Kinesis Firehose: {}", e);
    }

    Ok(username) 
}

// function that called by putRequest to get user count
async fn get_user_count(client: &Client, table_name: String) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    
    let mut count = 0;
    let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None; //mutable var

    loop {
        let mut scan_builder = client.scan().table_name(&table_name);

        if let Some(key) = &last_evaluated_key {
            for (k, v) in key {
                scan_builder = scan_builder.exclusive_start_key(k, v.clone());
            }
        }

        let resp = scan_builder.send().await?;

        count += resp.items.unwrap_or_default().len();

        if let Some(key) = resp.last_evaluated_key {
            last_evaluated_key = Some(key);
        } else {
            break;
        }
    }

    Ok(count)
}

// main function that runs appropraite lambda function
#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = aws_config::load_from_env().await; // gets config from environment.
    let db_client = Arc::new(Client::new(&config));  // creates the dynamdb client.

    run(service_fn(move |req: lambda_http::Request| {
        let db_client = Arc::clone(&db_client);  // prevent race condition.
        async move {
            match req.method().as_str() {
                "GET" => get_request(&*db_client, req).await,
                "PUT" => putRequest(&*db_client, req).await,
                _ => Ok(Response::new(Body::from("Method not allowed"))),
            }
        }
    })).await?;

    Ok(())
}
