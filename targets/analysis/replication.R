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

  

)
