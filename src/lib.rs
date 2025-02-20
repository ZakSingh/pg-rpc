#![feature(iterator_try_collect)]

use crate::codegen::codegen;
use crate::config::Config;
use crate::db::Db;
use crate::fn_index::FunctionIndex;
use crate::load_ddl::load_ddl;
use crate::rel_index::RelIndex;
use crate::ty_index::TypeIndex;
use std::fs;
use std::path::Path;
use std::time::Instant;

mod codegen;
mod config;
mod db;
mod exceptions;
mod fn_index;
mod fn_src_location;
mod ident;
mod load_ddl;
mod parse_domain;
mod pg_constraint;
mod pg_fn;
mod pg_id;
mod pg_image;
mod pg_rel;
mod pg_type;
mod pgsql_check;
mod rel_index;
mod sql_state;
mod tests;
mod ty_index;

pub fn run(schema_dir: &Path, output_path: &Path, config_path: &Path) -> anyhow::Result<()> {
  println!("Generating PgRPC functions...");
  let conf_str = fs::read_to_string(config_path).expect("Read config file failed");
  let config: Config = toml::from_str(&conf_str).expect("Failed to parse TOML");

  let start = Instant::now();

  let (src, fn_src_map) = load_ddl(schema_dir)?;

  let mut db = Db::new(&src);

  let rel_index = RelIndex::new(&mut db.client)?;


  let fn_index = FunctionIndex::new(&mut db.client, &rel_index, &config.schemas)?;
  let ty_index = TypeIndex::new(&mut db.client, fn_index.get_type_oids().as_slice())?;

  let mut err_count = 0;
  for f in fn_index.values() {
    if f.has_issues() {
      err_count += f.issues.len();
      f.report(&fn_src_map.get(&f.id()).unwrap());
    }
  }

  let code = codegen(&fn_index, &ty_index, &config)?;

  fs::write(output_path, code)?;

  if err_count > 0 {
    eprintln!("❌  {} problems detected by plpgsql_check", err_count);
  }

  let duration = start.elapsed().as_secs_f64();

  println!(
    "✅  {} PgRPC functions written to {} in {:.2}s",
    fn_index.len(),
    output_path.display(),
    duration
  );

  Ok(())
}
