use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{
  header::{HeaderMap, HeaderValue, CONTENT_TYPE},
  Body, Response,
};
use tokio::{fs::File, stream::StreamExt};
use tokio_util::codec::{BytesCodec, FramedRead};

const JSON_SIZE: usize = 500_000;
static FILE_PATH: &str = "big.json";
static URL: &str = "https://jsonplaceholder.typicode.com/posts";

fn new_progress_bar(total_size: u64) -> ProgressBar {
  let pb = ProgressBar::new(total_size);
  pb.set_style(
    ProgressStyle::default_bar()
      .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
      .progress_chars("#>-"),
  );
  pb
}

fn create_file(path: &str, size: usize) -> Result<(), std::io::Error> {
  let mut json = serde_json::map::Map::with_capacity(size);
  for i in 0..size {
    let k = format!("{}", i);
    let v = serde_json::Value::Number(i.into());
    let _ = json.insert(k, v);
  }
  let json = serde_json::Value::Object(json);
  let file = std::fs::File::create(path)?;
  let file = std::io::BufWriter::new(file);
  serde_json::to_writer(file, &json)?;
  Ok(())
}

async fn upload_file(path: &str, url: &str) -> Result<Response, Box<dyn std::error::Error>> {
  let file = File::open(path).await?;
  let metadata = file.metadata().await?;
  let file_size = metadata.len();
  let pb = new_progress_bar(file_size);
  let pb_clone = pb.clone();

  let mut stream = FramedRead::new(file, BytesCodec::new());
  let stream_progressed = async_stream::stream! {
    let mut last_inc = 0;
    while let Some(bytes) = stream.next().await {
      pb_clone.inc(last_inc);
      if let Ok(bytes) = &bytes {
        last_inc = bytes.len() as u64;
      }
      yield bytes;
    }
  };

  let body = Body::wrap_stream(stream_progressed);
  let mut headers = HeaderMap::new();
  headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
  let response = reqwest::Client::new().post(url).headers(headers).body(body).send().await?;
  pb.finish_with_message("uploaded");
  Ok(response)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  create_file(FILE_PATH, JSON_SIZE)?;
  let response = upload_file(FILE_PATH, URL).await?;
  println!("{:?}", response);
  Ok(())
}
