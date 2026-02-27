## targets/analysis/stage1_panel.R

box::use(
  magrittr[`%>%`], rlang[sym, syms]
)

stage1_panel_time_periods <- tibble::tribble(
  ~start_date,   ~end_date,    ~period_label,
  "1976-01-01",  "2024-12-31", "full_sample",
  "1976-01-01",  "1999-12-31", "pre_2000",
  "2000-01-01",  "2024-12-31", "post_2000",
  "2000-01-01",  "2011-12-31", "boom_bust",
  "2012-01-01",  "2024-12-31", "recovery"
)

values_stage1_sc_datasets <- tibble::tribble(
  ~name_suffix, ~dt_natl_hp_sym, ~geog_label, ~geog_yr, ~cluster_var, 

  # --- CBSA ---
  "fmcc_cbsa", sym("fmcc_natl_hp"), "cbsa", 2023, "GEOID", 
  "fhfa_cbsa", sym("fhfa_annual_hp_natl"), "cbsa", 2023, "GEOID",
  "zillow_cbsa", sym("zillow_natl_hp"), "cbsa", 2023, "GEOID",

  # --- County ---
  "fhfa_cnty", sym("fhfa_annual_hp_natl"), "cnty", 2020, "cz20", 
  "zillow_cnty", sym("zillow_natl_hp"), "cnty", 2020, "cz20",

  # --- Zip3 ---
  "fhfa_zip3", sym("fhfa_annual_hp_natl"), "zip3", 2000, "cz20",
  "fhfa_qtrly_zip3", sym("fhfa_hpi_qtr_natl"), "zip3", 2000, "cz20",

  # --- Zip5 ---
  "fhfa_zip5", sym("fhfa_annual_hp_natl"), "zip5", 2020, "cz20",
  "zillow_zip5", sym("zillow_natl_hp"), "zip5", 2020, "cz20",

  # --- Tract ---
  "fhfa_trct", sym("fhfa_annual_hp_natl"), "trct", 2020, "cz20",
) %>%
  dplyr::mutate(
    dt_lu_ml_sym = syms(sprintf("lu_ml_panel_%s", name_suffix)),
    dt_saiz_sym = syms(sprintf("saiz_%s_%d", tolower(geog_label), geog_yr)),
    dt_bsh_sym  = syms(sprintf("dt_bsh_%s_%d", tolower(geog_label), geog_yr)),
    dt_div_sym  = syms(sprintf("dt_division_%s_%d", tolower(geog_label), geog_yr)),
    ds_label = name_suffix
  )

values_stage1_sc_regressions <- values_stage1_sc_datasets %>%
  tidyr::crossing(stage1_panel_time_periods) %>%
  dplyr::mutate(
    # This symbol points to the targets created by the FIRST map below
    dt_prep_sym = rlang::syms(paste0("dt_panel_for_stage1_", name_suffix))
  )

stage1_sc_panel_targets <- list(
  
  # MAP 1: Data Preparation
  tar_map(
    values = values_stage1_sc_datasets,
    names = "name_suffix", 
    
    tar_target(
      dt_panel_for_stage1,
      f_prep_data_for_stage1_panel(
        dt_lu_ml_panel = dt_lu_ml_sym,
        dt_natl_hp     = dt_natl_hp_sym,
        dt_mtg         = dt_real_mtg_rate, # Constant
        dt_saiz        = dt_saiz_sym,
        dt_bsh         = dt_bsh_sym,
        dt_div         = dt_div_sym
      )
    )
  ),

  # MAP 2: Regressions
  tar_map(
    values = values_stage1_sc_regressions,
    names = c("name_suffix", "period_label"), 
    
    tar_target(
      reg_sc_panel_stage1, # e.g., reg_sc_panel_stage1_fmcc_cbsa_full_sample
      f_reg_sc_stage1_panel(
        dt                = dt_prep_sym,         # Points to Map 1 Output
        start_idx         = as.Date(start_date), 
        end_idx           = as.Date(end_date),   
        hp_target_var     = "hp.target",         
        geog_cluster_var  = cluster_var, 
        ds_label          = ds_label,
        geog_label        = geog_label
      ),
      format = "rds"
    )
  )
)
