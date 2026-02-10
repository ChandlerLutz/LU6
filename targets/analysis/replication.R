## targets/analysis/replication.R

box::use(
  data.table[...], magrittr[`%>%`]
)

replication_targets <- list(
  
  # 1. Prepare Data
  tar_target(
    dt_ms_replication_full,
    f_prep_ms_replication_data(
      dt_ms = dt_mian_sufi_lu_ml, 
      dt_bsh = dt_bsh_county_2010
    )
  ),

  # 2. Run Models
  tar_target(
    ms_replication_models,
    f_run_ms_models(dt_ms_replication_full),
    format = "rds"
  ),

  # 3. Generate Regression Stats (Renamed)
  tar_target(
    dt_ms_replication_regression_stats,
    f_create_ms_regression_stats(ms_replication_models)
  ),

  # 4. Generate TeX Table (Renamed)
  tar_target(
    ms_replication_tex_table,
    f_create_ms_tex_table(
      mods = ms_replication_models,
      dt_regression_stats = dt_ms_replication_regression_stats,
      output_tex = here::here("output-tex/ms_lu_main_table.tex")
    ),
    format = "file"
  )
)
