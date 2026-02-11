## targets/analysis/replication.R

box::use(
  data.table[...], magrittr[`%>%`]
)

replication_mian_suf_2014_targets <- list(
  
  tar_target(
    dt_ms_replication_full,
    f_prep_ms_replication_data(
      dt_ms = dt_mian_sufi_lu_ml, 
      dt_bsh = dt_bsh_county_2010
    )
  ),

  tar_target(
    ms_replication_models,
    f_run_ms_models(dt_ms_replication_full),
    format = "rds"
  ),

  tar_target(
    dt_ms_replication_regression_stats,
    f_create_ms_regression_stats(ms_replication_models)
  ),

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

replication_gurenetal_2021 <- list(

  # --- Data Prep & Merging ---
  # Merges your existing LU-ML predictions (dt_guren_et_al_lu_ml) 
  # with the full Guren data (for controls) and BSH data
  tar_target(
    dt_guren_merged,
    f_prep_guren_merged_data(
      dt_ml = dt_guren_et_al_lu_ml, 
      file_guren_raw = file_guren_replicate_fst,
      dt_bsh = dt_bsh_cbsa_2015
    )
  ),

  # --- Estimation ---
  # Runs the panel regressions (OLS, Saiz, BSH, LU-ML)
  tar_target(
    dt_guren_reg_results,
    f_estimate_guren_regressions(dt_guren_merged)
  ),

  # --- Table Output ---
  # Formats the results into a LaTeX table
  tar_target(
    file_guren_tex_table,
    f_write_guren_tex_table(
      DT.reg.output = dt_guren_reg_results,
      output_tex = here::here("output-tex/005-replicate/026-hw_panel_regs.tex")
    ),
    format = "file"
  )
)
