use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{
  header::{HeaderMap, HeaderValue, CONTENT_TYPE},
  Body, Response,
};
use tokio::{
  fs::File,
  stream::StreamExt,
  sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
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

async fn upload_file(
  file: File,
  url: &str,
  sender: UnboundedSender<u64>,
) -> Result<Response, Box<dyn std::error::Error>> {
  let mut stream = FramedRead::new(file, BytesCodec::new());
  let stream_progressed = async_stream::stream! {
    let mut last_inc = 0;
    while let Some(bytes) = stream.next().await {
      sender.send(last_inc).unwrap();
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
  Ok(response)
}

async fn progress_waiter(
  file_size: u64,
  mut receiver: UnboundedReceiver<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
  let pb = new_progress_bar(file_size);
  while let Some(p) = receiver.recv().await {
    pb.inc(p);
  }
  pb.finish_with_message("uploaded");
  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  create_file(FILE_PATH, JSON_SIZE)?;

  let file = File::open(FILE_PATH).await?;
  let metadata = file.metadata().await?;
  let file_size = metadata.len();

  let (sender, receiver) = unbounded_channel();
  let response = tokio::try_join!(progress_waiter(file_size, receiver), upload_file(file, URL, sender))?.1;
  println!("{:?}", response);

  Ok(())
}
