## targets/analysis/replication.R

box::use(
  data.table[...], magrittr[`%>%`]
)

replication_mian_sufi_2014_targets <- list(
  
  tar_target(
    dt_ms_replication_full,
    f_prep_ms_replication_data(
      dt_ms = dt_mian_sufi_lu_ml, 
      dt_bsh = dt_bsh_cnty_2010
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


replication_gurenetal_2021_targets <- list(
  tar_target(
    guren_etal_tab1_models,
    f_est_guren_etal_tab1_models(file_guren_replicate_fst),
    format = "rds"
  ),
  tar_target(
    guren_etal_tab1_tex,
    create_guren_etal_tab1_tex(
      guren_etal_tab1_models,
      file_out = here::here("output-tex/guren_etal_tab1_rep.tex")
    ),
    format = "file"
  ),
  tar_target(
    dt_hw_elast_lu_reg_felm_models,
    est_gurenetal_sc_lu_ml_reg_models(
      file_guren_replicate = file_guren_replicate_fst,
      dt_lu_ml = dt_guren_et_al_lu_ml, 
      dt_bsh = dt_bsh_cbsa_2015        
    ),
    format = "rds"
  ),
  tar_target(
    dt_gurenetal_sc_reg_mod_stats,
    extract_gurenetal_sc_lu_ml_reg_mod_stats(dt_hw_elast_lu_reg_felm_models),
    format = "parquet"
  ),
  tar_target(
    dt_hw_elast_lu_reg_tex,
    create_gurenetal_sc_lu_reg_tex(
      dt_reg_output = dt_hw_elast_lu_reg_felm_models,
      file_out = here::here("output-tex/gurenetal_sc_lu_panel_regs.tex")
    ),
    format = "file"
  )
)

replication_chaneyetal_2012_targets <- list(
  tar_target(
    dt_chaneyetal_sc_lu_ml_models,
    est_chaneyetal_sc_lu_ml_models(
      dt_chaneyetal_lu_ml = dt_chaney_et_al_lu_ml,
      file_raw_chaney = file_raw_chaney_2012, 
      dt_bsh_data = dt_bsh_cbsa_2009,
      dt_real_mtg_rate = dt_real_mtg_rate
    ),
    format = "rds"
  ),
  tar_target(
    chaneyetal_sc_lu_ml_tex,
    create_chaneyetal_sc_lu_ml_tex(
      est_output = dt_chaneyetal_sc_lu_ml_models,
      file_out = here::here("output-tex/chaneyetal_sc_lu_regs.tex")
    ),
    format = "file"
  ),
  tar_target(
    dt_chaneyetal_sc_lu_ml_reg_stats,
    extract_chaneyetal_sc_lu_ml_reg_stats(
      est_output = dt_chaneyetal_sc_lu_ml_models
    ),
    format = "parquet"
  )
)

replication_stroebelvavra_2019_targets <- list(
  tar_target(
    dt_sv_2019_models,
    est_stroebel_vavra_2019_models(
      dt_lu_01_06 = dt_stroebel_vavra_01_06_lu_ml,
      dt_lu_07_11 = dt_stroebel_vavra_07_11_lu_ml,
      file_raw_01_06 = file_raw_sv_01_06,
      file_raw_07_11 = file_raw_sv_07_11,
      dt_bsh_data = dt_bsh_cbsa_2023
    ),
    format = "rds"
  ),
  tar_target(
    sv_2019_tex,
    create_stroebel_vavra_2019_tex(
      est_output = dt_sv_2019_models,
      file_out = here::here("output-tex/stroebel_vavra_2019_sc_lu_regs.tex")
    ),
    format = "file"
  ),
  tar_target(
    dt_sv_2019_stats,
    extract_stroebel_vavra_2019_stats(dt_sv_2019_models),
    format = "parquet"
  )
)
