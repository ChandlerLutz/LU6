## core/test_helpers.R

f_extract_stable_features <- function(obj) {
  
  box::use(
    data.table[...], magrittr[`%>%`]
  )
  
  if (inherits(obj, "felm") || inherits(obj, "feols")) {
    
    estimates <- as.numeric(coef(obj))
    terms <- names(coef(obj))
    
    if (inherits(obj, "felm")) {
      ses <- if (!is.null(obj$cse)) {
        as.numeric(obj$cse)
      } else if (!is.null(obj$rse)) {
        as.numeric(obj$rse)
      } else {
        as.numeric(obj$se)
      }
    } else {
      ses <- as.numeric(obj$se)
    }
    
    obj_dt <- data.table(
      term = terms,
      estimate = estimates,
      std_error = ses
    )
    
    obj_dt[, estimate := round(estimate, 4)]
    obj_dt[, std_error := round(std_error, 4)]
    
    return(obj_dt)
      
  } else if (inherits(obj, "data.table") || inherits(obj, "data.frame")) {
    obj <- obj %>%
      as.data.table() %>%
      copy() %>%
      .[, names(.SD) := NULL, .SDcols = is.list] %>%
      .[, names(.SD) := lapply(.SD, \(x) round(x, 4)), .SDcols = is.numeric]
    
    setorder(obj)
    
    return(head(obj, 10))
    
  } else if (inherits(obj, "ggplot")) {

    orig_rows <- nrow(obj)
    orig_cols <- ncol(obj)
    
    obj <- obj %>%
      as.data.table() %>%
      copy() %>%
      .[, names(.SD) := NULL, .SDcols = is.list] %>%
      .[, names(.SD) := lapply(.SD, \(x) round(x, 4)), .SDcols = is.numeric]
    
    setorder(obj)
    
    return(list(
      dimensions = data.table(rows = orig_rows, columns = orig_cols),
      top_10_rows = head(obj, 10)
    ))
    
  } else if (is.list(obj)) {
    return(lapply(obj, f_extract_stable_features))
  } else {
    return(obj)
  }
}


f_run_snapshot_tests <- function(obj, test_name) {

  # Temporarily disable scientific notation
  old_opts <- options(scipen = 999)
  # Ensure it resets to normal when the function finishes
  on.exit(options(old_opts))
  
  # The directory where our custom snapshots will live
  snap_dir <- "targets/tests/snapshots"
  dir.create(snap_dir, showWarnings = FALSE, recursive = TRUE)
  
  snap_file <- file.path(snap_dir, paste0(test_name, ".md"))
  
  stable_output <- f_extract_stable_features(obj)
  
  # Capture how the output looks exactly as it prints in the console
  new_snapshot <- capture.output(print(stable_output))
  
  # 1. If this is the first run, save the file and pass the test
  if (!file.exists(snap_file)) {
    writeLines(new_snapshot, snap_file)
    message("✔ Initial snapshot created for: ", test_name)
    return(TRUE)
  }
  
  # 2. If it exists, read the old snapshot and compare
  old_snapshot <- readLines(snap_file)
  
  if (!identical(new_snapshot, old_snapshot)) {
    
    # Check if we are in "Update Mode" via the environment variable
    if (identical(Sys.getenv("TESTTHAT_UPDATE_SNAPSHOTS"), "true")) {
      writeLines(new_snapshot, snap_file)
      message("✔ Snapshot intentionally updated for: ", test_name)
      
    } else {
      # Throw an error to halt the pipeline, just like testthat would
      stop(
        "\n✖ Snapshot mismatch for: ", test_name, "!\n",
        "The model output has changed. To review the changes, check your Git diff.\n",
        "To accept the new results, run this in your console:\n\n",
        "  Sys.setenv(TESTTHAT_UPDATE_SNAPSHOTS = 'true')\n",
        "  targets::tar_make()\n",
        "  Sys.setenv(TESTTHAT_UPDATE_SNAPSHOTS = '')\n"
      )
    }
  }
  
  return(TRUE) 
}
