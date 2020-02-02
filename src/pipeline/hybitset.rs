use std::path::PathBuf;
use std::error::Error;

use tokio::fs::{ File, OpenOptions };
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use riven::consts::Region;

use crate::util::hybitset::HyBitSet;
use crate::util::time;
use crate::util::file_find;
use crate::util::error::PbwError;
use crate::dyn_err;

const FILE_TAG: &'static str = "match_hbs";
const FILE_EXT: &'static str = "json";

// TODO: really need to distinguish between "no files found" and "it fucked up".
pub async fn read_match_hybitset(region: Region) -> Result<HyBitSet, Box<dyn Error + Send>> {
    
    let path = file_find::find_latest(region, FILE_TAG, FILE_EXT)
        .ok_or(dyn_err(PbwError::new("Failed to find match file.".to_owned())))?;

    let mut file = File::open(path).await.map_err(dyn_err)?;
    let mut bytes = vec![];
    file.read_to_end(&mut bytes).await.map_err(dyn_err)?;

    let hbs = serde_json::from_slice(&bytes).map_err(dyn_err)?;
    Ok(hbs)
}

pub async fn write_match_hybitset(region: Region, match_hbs: &HyBitSet) -> Result<(), Box<dyn Error>> {
    
    let path: PathBuf = [
        "data",
        &format!("{:?}", region).to_lowercase(),
        &format!("{}.{}.{}", FILE_TAG, time::datetimestamp(), FILE_EXT),
    ].iter().collect();

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path).await?;

    let bytes = serde_json::ser::to_vec_pretty(match_hbs)?;
    file.write_all(&bytes).await?;
    file.shutdown().await?;

    Ok(())
}
