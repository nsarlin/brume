use ncclient::concrete::remote::{RemoteFs, RemoteFsError};

#[tokio::main]
async fn main() -> Result<(), RemoteFsError> {
    let remote = RemoteFs::new("http://localhost:8080", "admin", "admin").await?;

    dbg!(remote);

    Ok(())
}
