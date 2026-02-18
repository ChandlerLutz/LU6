## targets/lu_ml.R

box::use(
  data.table[...], magrittr[`%>%`], rlang[sym]
)

values_lu_ml_panel_inputs <- tibble::tribble(
  ~name_suffix,  ~dt_hp_sym, ~dt_lu_file_sym, ~lu_id_col,
  ~dt_pci_sym, ~dt_gmaps_sym,
  ~dt_tract_cw_sym,
  
  # FMCC
  "fmcc_cbsa", sym("fmcc_cbsa_hp"), sym("file_raw_lu_cbsa_2022"), "GEOID",
  sym("bea_regional_pci_cbsa_2020"), sym("gmaps_region_amenity_demand_cbsa_2020"),
  NULL,

  # FHFA Annual
  "fhfa_cbsa", sym("fhfa_annual_hp_cbsa"), sym("file_raw_lu_cbsa_2020"), "GEOID",
  sym("bea_regional_pci_cbsa_2020"), sym("gmaps_region_amenity_demand_cbsa_2020"),
  NULL,

  "fhfa_cnty", sym("fhfa_annual_hp_cnty"), sym("file_raw_lu_cnty_2020"), "GEOID",
  sym("bea_regional_pci_county_2020"), sym("gmaps_region_amenity_demand_county_2020"),
  NULL,

  "fhfa_zip5", sym("fhfa_annual_hp_zip5"), sym("file_raw_lu_zip5_2020"), "zip5",
  sym("bea_regional_pci_zip5_2020"), sym("gmaps_region_amenity_demand_zip5_2020"),
  NULL,

  "fhfa_zip3", sym("fhfa_annual_hp_zip3"), sym("file_raw_lu_zip3"), "zip3",
  sym("bea_regional_pci_zip3_2000"), sym("gmaps_region_amenity_demand_zip3_2000"),
  NULL,

  "fhfa_trct", sym("fhfa_annual_hp_trct"), sym("file_raw_lu_trct"), "NHGISCODE",
  sym("bea_regional_pci_tract_2020"), sym("gmaps_region_amenity_demand_tract_2020"),
  sym("cw_trct_nhgis_trct"),

  # FHFA Quarterly
  "fhfa_qtrly_zip3", sym("fhfa_hpi_qtr_zip3"), sym("file_raw_lu_zip3"), "zip3",
  sym("bea_regional_pci_zip3_2000"), sym("gmaps_region_amenity_demand_zip3_2000"),
  NULL,

  # Zillow
  "zillow_cbsa", sym("zillow_hp_cbsa"), sym("file_raw_lu_cbsa_2022"), "GEOID",
  sym("bea_regional_pci_cbsa_2022"), sym("gmaps_region_amenity_demand_cbsa_2022"),
  NULL,

  "zillow_cnty", sym("zillow_hp_county"), sym("file_raw_lu_cnty_2020"), "GEOID",
  sym("bea_regional_pci_county_2020"), sym("gmaps_region_amenity_demand_county_2020"),
  NULL,

  "zillow_zip5", sym("zillow_hp_zip"), sym("file_raw_lu_zip5_2020"), "zip5",
  sym("bea_regional_pci_zip5_2020"), sym("gmaps_region_amenity_demand_zip5_2020"),
  NULL
)


f_get_lu_raw_file_loc <- function(file_loc) {

  path <- sprintf("../LandUnavailability/work/analyze/%s", file_loc)

  if (!file.exists(path)) 
    stop(sprintf("External dependency missing: %s", path))
  
  return(path)
}


lu_ml_targets <- list(

  ## The housing market cycles for panel analysis
  tar_target(
    housing_cycles_panel,
    data.table(
      cycle_start_yr = c(1970, 1980, 1990, 2000, 2008, 2013, 2020),
      cycle_end_yr = c(1979, 1989, 1999, 2007, 2012, 2019, 2029),
      cycle_label = c(
        "1970s", "1980s", "1990s", "2000-2007", "2008-2012", "2013-2019", "2020s"
      )
    ) %>%
      .[, let(cycle_start_date = as.Date(sprintf("%d-01-01", cycle_start_yr)),
              cycle_end_date = as.Date(sprintf("%d-12-31", cycle_end_yr)))] %>%
      setcolorder(c("cycle_start_yr", "cycle_start_date", "cycle_end_yr",
                    "cycle_end_date"))
    ), 
  
  ## LU-ML Raw Files
  tar_target(
    file_raw_lu_cbsa_2009,
    f_get_lu_raw_file_loc("110-cbsa/cbsa_shp_last_yr=2023/cbsa_lu_2009.parquet"),
    format = "file"
  ),
  tar_target(
    file_raw_lu_cbsa_2015,
    f_get_lu_raw_file_loc("110-cbsa/cbsa_shp_last_yr=2023/cbsa_lu_2015.parquet"),
    format = "file"
  ),
  tar_target(
    file_raw_lu_cbsa_2020,
    f_get_lu_raw_file_loc("110-cbsa/cbsa_shp_last_yr=2023/cbsa_lu_2020.parquet"),
    format="file"
  ),
  tar_target(
    file_raw_lu_cbsa_2022,
    f_get_lu_raw_file_loc("110-cbsa/cbsa_shp_last_yr=2023/cbsa_lu_2022.parquet"), 
    format = "file"
  ),
  tar_target(
    file_raw_lu_cbsa_2023,
    f_get_lu_raw_file_loc("110-cbsa/cbsa_shp_last_yr=2023/cbsa_lu_2023.parquet"),
    format = "file"
  ),
  tar_target(
    file_raw_lu_cnty_2010,
    f_get_lu_raw_file_loc("120-cnty-lu/cnty_shp_last_yr=2023/cnty_lu_2010.parquet"),
    format="file"
  ),
  tar_target(
    file_raw_lu_cnty_2020,
    f_get_lu_raw_file_loc("120-cnty-lu/cnty_shp_last_yr=2023/cnty_lu_2020.parquet"),
    format="file"
  ),
  tar_target(
    file_raw_lu_zip5_2020, 
    f_get_lu_raw_file_loc("051-zip2020/zip2020_lu_all.parquet"),
    format="file"
  ),
  tar_target(
    file_raw_lu_zip3, 
    f_get_lu_raw_file_loc("055-zip3/zip3_lu_all.parquet"), 
    format="file"
  ),
  tar_target(
    file_raw_lu_trct,
    f_get_lu_raw_file_loc("060-tract/tract_shp_last_yr=2023/tract_lu_all.parquet"),
    format="file"
  ),

  tar_map(
    values = values_lu_ml_panel_inputs,
    names = "name_suffix", # Results in targets like: lu_ml_panel_fmcc_cbsa

    tar_target(
      lu_ml_panel,
      f_get_universal_lu_ml_panel(
        dt_hp                   = dt_hp_sym,
        dt_regional_pci_panel   = dt_pci_sym,
        dt_gmaps_amenity_demand = dt_gmaps_sym,
        dt_hm_cycles            = housing_cycles_panel,
        
        # Handle the nested LU Prep function dynamically
        dt_lu = f_prep_lu_data(
          file_path    = dt_lu_file_sym, 
          geog_id_col  = lu_id_col,
          # Pass the crosswalk symbol (or NULL) explicitly
          dt_tract_cw  = dt_tract_cw_sym
        )
      )
    )
  ), 

  ## Mian and Sufi (2014) LU-ML
  tar_target(
    dt_mian_sufi_lu_ml,
    f_get_mian_sufi_lu_ml(
      file_path_ms = file_raw_mian_sufi_2014,
      dt_lu = f_prep_lu_data(file_raw_lu_cnty_2010, geog_id_col = "GEOID"),
      dt_regional_pci_panel = bea_regional_pci_county_2010,
      dt_gmaps_amenity_demand = gmaps_region_amenity_demand_county_2023
    )
  ),

  ## Guren et al. (2021) LU-ML
  tar_target(
    dt_guren_et_al_lu_ml,
    f_get_guren_et_al_lu_ml(
      file_path_guren = file_guren_replicate_fst,
      dt_lu = f_prep_lu_data(file_raw_lu_cbsa_2015, geog_id_col = "GEOID"),
      dt_regional_pci_panel = bea_regional_pci_cbsa_2015,
      dt_gmaps_amenity_demand = gmaps_region_amenity_demand_cbsa_2015, 
      dt_hm_cycles = housing_cycles_panel
    )
  ),

  ## Chaney et al. (2012) Replication LU-ML
  tar_target(
    dt_chaney_et_al_lu_ml,
    f_get_chaney_et_al_lu_ml(
      file_path_chaneyetal = file_raw_chaney_2012,
      dt_lu_base = f_prep_lu_data(file_raw_lu_cbsa_2009, geog_id_col = "GEOID"),
      dt_regional_pci_panel = bea_regional_pci_cbsa_2020,
      dt_gmaps_amenity_demand = gmaps_region_amenity_demand_cbsa_2020,
      dt_hm_cycles = housing_cycles_panel
    )
  ),

  ## Stroebel and Vavra (2019) LU-ML
  tar_target(
    dt_stroebel_vavra_01_06_lu_ml,
    f_get_stroebel_vavra_01_06_lu_ml(
      file_path_hp = file_raw_sv_01_06,
      dt_lu = f_prep_lu_data(file_raw_lu_cbsa_2023, geog_id_col = "GEOID"),
      dt_regional_pci_panel = bea_regional_pci_cbsa_2023,
      dt_gmaps_amenity_demand = gmaps_region_amenity_demand_cbsa_2023
    )
  ),
  tar_target(
    dt_stroebel_vavra_07_11_lu_ml,
    f_get_stroebel_vavra_07_11_lu_ml(
      file_path_hp = file_raw_sv_07_11,
      dt_lu = f_prep_lu_data(file_raw_lu_cbsa_2023, geog_id_col = "GEOID"),
      dt_regional_pci_panel = bea_regional_pci_cbsa_2023,
      dt_gmaps_amenity_demand = gmaps_region_amenity_demand_cbsa_2023
    )
  )

)

