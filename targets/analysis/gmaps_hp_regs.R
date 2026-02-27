## targets/analysis/gmaps_hp_regs.R

gmps_hp_regs_targets <- list(
  tarchetypes::tar_map(
    values = tibble::tibble(
      ds_name = c("fmcc_cbsa", "fhfa_cbsa", "zillow_cbsa"),
      hp_ds = rlang::syms(c("fmcc_cbsa_hp", "fhfa_annual_hp_cbsa", "zillow_hp_cbsa")),
    ),
    names = ds_name,
    tar_target(
      gmaps_hp_reg_models,
      f_reg_hp_gmaps_amenity_models(
        dt_hp_panel = hp_ds, 
        dt_gmaps_amenities = gmaps_region_amenity_demand_cbsa_2023, 
        dt_hm_cycles = dt_housing_cycles_panel
      ),
      format = "rds"
    )
  ),

  tar_target(
    gmaps_hp_reg_plot,
    f_plot_gmaps_amenity_regs(
      mods_fmcc_cbsa = gmaps_hp_reg_models_fmcc_cbsa, 
      mods_fhfa_cbsa = gmaps_hp_reg_models_fhfa_cbsa, 
      mods_zillow_cbsa = gmaps_hp_reg_models_zillow_cbsa,
      output_plot = "output-plots/gmaps_amenity_hp_regs.pdf"
    ),
    format = "file"
  )
)
