read_dbc <- function(filepath) {
  print(paste("Reading", filepath))
  read.dbc::read.dbc(filepath, as.is = TRUE) |>
    # Remove non-ASCII characters by converting to ASCII to avoid issues with parquet
    # This is a workaround for issues with non-ASCII characters in parquet files
    dplyr::mutate(
      dplyr::across(
        dplyr::everything(),
        function(x) iconv(x, "latin1", "ASCII", sub = "")
      )
    )
}

write_parquet <- function(data, filepath) {
  print(paste("Writing parquet", filepath))
  arrow::write_parquet(data, filepath, compression = "snappy")
}

parse_filename <- function(filepath) {
  # Extract the filename without the directory path
  filename <- fs::path_file(filepath) |> fs::path_ext_remove()
  # Format: <dataset>_<date[-<uf>[-<version>]]>_<modification>
  parts <- strsplit(filename, "_")[[1]]
  dataset <- parts[1]
  partition_parts <- strsplit(parts[2], "-")[[1]]
  date <- partition_parts[1]
  uf <- ifelse(length(partition_parts) > 1, partition_parts[2], NA)
  version <- ifelse(length(parts) > 3, parts[3], NA)
  modification <- parts[3]
  list(
    filepath = filepath,
    filename = filename,
    dataset = dataset,
    date = date,
    uf = uf,
    version = version,
    modification = modification
  )
}

process_dataset <- function(diretorio, dest_dir) {
  dataset <- fs::path_file(diretorio)
  for (date_dir in fs::dir_ls(diretorio)) {
    date <- fs::path_file(date_dir)
    output_dir <- fs::path(dest_dir, dataset)

    # List all DBC files in the date directory
    dbc_files <- fs::dir_ls(date_dir, glob = "*.dbc")

    # Get the latest modification date from this date directory
    latest_modification <- dbc_files |>
      purrr::map_dfr(parse_filename) |>
      dplyr::pull("modification") |>
      max()

    # Construct the output file path
    # Format: <dataset>_<date>_<latest_modification>.parquet
    output_file <- fs::path(
      output_dir,
      paste0(dataset, "_", date, "_", latest_modification, ".parquet")
    )
    # Check if the output file already exists
    if (fs::file_exists(output_file)) {
      print(paste("File", output_file, "already exists. Skipping..."))
      next
    }

    print(paste("Processing", date))
    # Create the output directory
    fs::dir_create(output_dir)
    # Read all DBC files in the date directory and write to a single Parquet file
    tryCatch(
      {
        dbc_files |>
          purrr::map_dfr(read_dbc) |>
          write_parquet(output_file)
      },
      error = function(e) {
        print(e)
      }
    )

  }

  print(paste("Finished processing dataset:", dataset))

}
