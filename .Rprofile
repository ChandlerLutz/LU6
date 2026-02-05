# --- renv activation ---
if (file.exists("renv/activate.R")) {
  source("renv/activate.R")
} else {
  # Try to find the root if we're in a subdirectory (like /data-raw/ or a worker)
  proj_root <- tryCatch(here::here(), error = function(e) ".")
  act_path <- file.path(proj_root, "renv/activate.R")
  
  if (file.exists(act_path)) {
    source(act_path)
  }
  # No else/stop here — let the session start so we can at least debug
}
