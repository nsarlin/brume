use ncclient::RemoteDir;
use reqwest_dav::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let remote = RemoteDir::new("http://localhost:8080", "admin", "admin").await?;

    dbg!(remote);

    Ok(())
}
