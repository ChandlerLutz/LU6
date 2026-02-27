## targets/analysis/income_elasticity_targets.R

income_elasticity_targets <- list(
  tar_target(
    mods_income_elast_hu_cbsa_2023,
    f_est_income_elasticity_hu(
      est_label = "income_elast_hu_cbsa_2023",
      dt_lu_ml_hu = lu_ml_hu_decadal_panel_cbsa_2023,
      file_bea_income = file_raw_bea_cbsa2023, 
      dt_bsh = dt_bsh_cbsa_2020,
      dt_saiz = saiz_cbsa_2020,
      geog_cluster_var = "GEOID"
      ),
    format = "rds"
  ),

  tar_map(
    values = tibble::tibble(
      est_label = c("fmcc", "fhfa"),
      dt_hp_panel = rlang::syms(
        c("fmcc_cbsa_hp", "fhfa_annual_hp_cbsa")
      )
    ),
    names = est_label,
    tar_target(
      mods_income_elast_hp_fmcc_cbsas,
      f_est_income_elast_hp(
        est_label = est_label,
        dt_hp_panel = dt_hp_panel,
        dt_lu_ml_hu = lu_ml_hu_decadal_panel_cbsa_2023,
        file_bea_income = file_raw_bea_cbsa2023, 
        dt_bsh = dt_bsh_cbsa_2023,
        dt_saiz = saiz_cbsa_2023,
        geog_cluster_var = "GEOID",
        dt_cw_to_st = cw_cbsa_2023_state,
        dt_geoids_to_keep = fmcc_cbsa_hp
      ),
      format = "rds"
    )
  ),

  tar_target(
    mods_income_elast_hp_fhfa_cbsas,
    f_est_income_elast_hp(
      est_label = "fhfa_cbsas",
      dt_hp_panel = fhfa_annual_hp_cbsa,
      dt_lu_ml_hu = lu_ml_hu_decadal_panel_cbsa_2023,
      file_bea_income = file_raw_bea_cbsa2023, 
      dt_bsh = dt_bsh_cbsa_2023,
      dt_saiz = saiz_cbsa_2023,
      geog_cluster_var = "GEOID",
      dt_cw_to_st = cw_cbsa_2023_state
    ),
    format = "rds"
  ),

  tar_target(
    mods_income_elast_hp_fhfa_cntys,
    f_est_income_elast_hp(
      est_label = "fhfa_cntys",
      dt_hp_panel = fhfa_annual_hp_cnty,
      dt_lu_ml_hu = lu_ml_hu_decadal_panel_cnty_2020,
      file_bea_income = file_raw_bea_cnty2020, 
      dt_bsh = dt_bsh_cnty_2020,
      dt_saiz = saiz_cnty_2020,
      geog_cluster_var = "GEOID",
      dt_cw_to_st = cw_cnty_2020_state
    ),
    format = "rds"
  ),

  
  tar_target(
    p_income_elast_intro_fig,
    f_make_income_elast_intro_fig(
      dt_mods_hp = mods_income_elast_hp_fmcc_cbsas_fmcc,
      dt_mods_hu = mods_income_elast_hu_cbsa_2023,
      fe_string = "year_char", 
      output_plot = "output-plots/income_elasticity_intro_fig.pdf"
    ),
    format = "file"
  ),
    tar_target(
      p_income_elast_intro_fig_with_cbsa_fe,
    f_make_income_elast_intro_fig(
      dt_mods_hp = mods_income_elast_hp_fmcc_cbsas_fmcc,
      dt_mods_hu = mods_income_elast_hu_cbsa_2023,
      fe_string = "GEOID + year_char", 
      output_plot = "output-plots/income_elasticity_intro_fig_with_cbsa_fe.pdf"
    ),
    format = "file"
  )
)
