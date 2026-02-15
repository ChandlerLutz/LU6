## targets/analysis/stage1_panel.R

stage1_panel_targets <- list(
  
  tar_target(
    stage1_panel_time_periods,
    data.table(
      start_idx = as.Date(c("1976-01-01", "1976-01-01", "2000-01-01", "2000-01-01",
                            "2012-01-01")),
      end_idx   = as.Date(c("2024-12-01", "1999-12-01", "2024-12-01", "2011-12-01",
                            "2024-12-01"))
    )
  )
)
