use crate::fn_index::FunctionId;
use crate::fn_src_location::{get_function_spans, SrcLoc};
use ariadne::sources;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::{fs, io};

/// Combine the sql files into one string ready to be executed
/// and find the src locations of all functions defined in the files
pub fn load_ddl(dir: impl AsRef<Path>) -> io::Result<(String, HashMap<FunctionId, SrcLoc>)> {
    let paths = list_sql_files(dir)?;
    let mut combined = String::new();
    // Lookups will be done via function name, so it should be fnid -> src, path
    let mut fn_locs = HashMap::new();

    for path in paths {
        let content = fs::read_to_string(&path)?;
        let fn_spans = get_function_spans(&content)?;
        fn_locs.extend(
            fn_spans
                .into_iter()
                .map(|(fn_id, span)| (fn_id, (path.clone(), span))),
        );
        combined.push_str(&content);
        // add newline between files
        combined.push('\n');
    }

    Ok((combined, fn_locs))
}

/// Recursively traverse a directory and return all .sql file paths in order.
fn list_sql_files(path: impl AsRef<Path>) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let path = path.as_ref();

    if path.is_dir() {
        let mut entries = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            let path = entry.path();

            if path.is_dir() {
                files.extend(list_sql_files(&path)?);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("sql") {
                files.push(path);
            }
        }
    }

    Ok(files)
}
