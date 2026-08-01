use std::{fs::File, io::Read, path::Path};

use anyhow::Result;
use libautark::model::project::ProjectData;

fn save_project_data(proj: ProjectData, path: &Path) -> Result<()> {
    let mut file = File::create(path)?;
    todo!()
    // proj.serialize(&mut Serializer::new(&mut file))?;
    // Ok(( )
}

fn open_project_data(path: &Path) -> Result<ProjectData> {
    let mut buf = vec![];
    let mut file = File::open(path)?;
    file.read_to_end(&mut buf)?;
    todo!()
    // let content: ProjectData = rmp_serde::decode::from_slice(&buf)?;
    // Ok(content)
}
