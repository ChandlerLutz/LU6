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
  ),

  ## FMCC Panel
  tarchetypes::tar_map(
    tar_target(
      stage1_panel_fmcc_reg_results,
    f_reg_sc_stage1_panel(
      dt = ,
      start_idx = stage1_panel_time_periods$start_idx,
      end_idx = stage1_panel_time_periods$end_idx,
      hp_target_var = "hp_fmcc",
      control_vars = c("median_income", "population"),
      geog_cluster_var = "division_idx",
      ds_label = "FMCC Panel",
      geog_label = "Division"
    )
  )
