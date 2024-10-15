use ncclient::concrete::{
    remote::{RemoteFs, RemoteFsError},
    ConcreteFS,
};

#[tokio::main]
async fn main() -> Result<(), RemoteFsError> {
    let remote = RemoteFs::new("http://localhost:8080", "admin", "admin")?;

    let vfs = remote.load_virtual().await?;

    let mut f = vfs
        .concrete()
        .open("/Readme.md".try_into().unwrap())
        .await
        .unwrap();

    vfs.concrete()
        .write("/Readmd.me".try_into().unwrap(), &mut f)
        .await
        .unwrap();
    Ok(())
}
