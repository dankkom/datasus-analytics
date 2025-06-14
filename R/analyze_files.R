# Analyze the columns of a dbc file, return a dataframe with
# filename, column name, and number of non-null values
# Args:
#   filepath: A string of the file path
# Returns:
#   A dataframe with column name and filename
#
# Example:
# analyze_file_columns("data/datasus/sih-rd/199306/sih-rd_199306-ac_20131031.dbc")
#                                                       file column_name non_null_values
# 1 D:/datasus/sih-rd/199306/sih-rd_199306-ac_20131031.dbc       UF_ZI            2854
# 2 D:/datasus/sih-rd/199306/sih-rd_199306-ac_20131031.dbc    ANO_CMPT            2854
# 3 D:/datasus/sih-rd/199306/sih-rd_199306-ac_20131031.dbc    MES_CMPT            2854
# 4 D:/datasus/sih-rd/199306/sih-rd_199306-ac_20131031.dbc       ESPEC            2854
# 5 D:/datasus/sih-rd/199306/sih-rd_199306-ac_20131031.dbc    CGC_HOSP            2854
analyze_file_columns <- function(filepath) {
  print(filepath)
  # Read the dbc file
  d <- tryCatch(
    read.dbc::read.dbc(filepath, as.is = TRUE),
    error = function(cond) {
      message(conditionMessage(cond))
      NULL
    }
  )

  # Parse file name to get year, month and UF
  filename <- fs::path_ext_remove(basename(filepath))
  filename_parts <- strsplit(filename, "_")[[1]]
  time_region_parts <- strsplit(filename_parts[2], "-")[[1]]
  time_part <- time_region_parts[1]
  region_part <- time_region_parts[2]

  # Get the column names
  if (!is.null(d)) {
    df_file_columns <- data.frame(
      file = filename,
      time = time_part,
      region = region_part,
      column_name = names(d),
      non_null_values = sapply(d, function(x) sum(!is.na(x))),
      null_values = sapply(d, function(x) sum(is.na(x))),
      stringsAsFactors = FALSE
    )
  } else {
    df_file_columns <- data.frame(
      file = filename,
      time = time_part,
      region = region_part,
      column_name = NA,
      non_null_values = NA,
      null_values = NA,
      stringsAsFactors = FALSE
    )
  }

  rownames(df_file_columns) <- NULL
  # Append the dataframe to df_columns
  df_file_columns
}