## targets/lu_ml.R

box::use(
  data.table[...], magrittr[`%>%`]
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
    file_raw_lu_cbsa_2020,
    f_get_lu_raw_file_loc("110-cbsa/cbsa_shp_last_yr=2023/cbsa_lu_2020.parquet"),
    format="file"),
  tar_target(
    file_raw_lu_cbsa_2022,
    f_get_lu_raw_file_loc("110-cbsa/cbsa_shp_last_yr=2023/cbsa_lu_2022.parquet"), 
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
    file_raw_lu_zip5_2020, f_get_lu_raw_file_loc("051-zip2020/zip2020_lu_all.parquet"),
    format="file"
  ),
  tar_target(
    file_raw_lu_zip3, f_get_lu_raw_file_loc("055-zip3/zip3_lu_all.parquet"), format="file"
  ),
  tar_target(
    file_raw_lu_trct,
    f_get_lu_raw_file_loc("060-tract/tract_shp_last_yr=2023/tract_lu_all.parquet"),
    format="file"
  ),

  ## FMCC LU-ML panel
  tar_target(
    fmcc_cbsa_hpi_lu_ml_panel,
    f_get_fmcc_cbsa_hpi_lu_ml_panel(
      dt_fmcc_hp_panel = fmcc_cbsa_hp,
      file_path_lu = file_raw_lu_cbsa_2022,
      dt_regional_pci_panel = bea_regional_pci_cbsa_2020,
      dt_hm_cycles = housing_cycles_panel,
      dt_cbsa_shp = cbsa_shp_2022
    )
  ), 

  ## FHFA Annual LU-ML panel
  tar_target(
    fhfa_hpi_lu_ml_cbsa_panel,
    f_get_fhfa_cbsa_annual_lu_ml_panel(
      dt_hp = fhfa_annual_hp_cbsa,
      file_path_lu = file_raw_lu_cbsa_2020,
      dt_regional_pci_panel = bea_regional_pci_cbsa_2020,
      dt_hm_cycles = housing_cycles_panel,
      dt_shp = cbsa_shp_2020
    )
  ),
  tar_target(
    fhfa_hpi_lu_ml_cnty_panel,
    f_get_fhfa_county_annual_lu_ml_panel(
      dt_hp = fhfa_annual_hp_cnty,
      file_path_lu = file_raw_lu_cnty_2020,
      dt_regional_pci_panel = bea_regional_pci_county_2020,
      dt_hm_cycles = housing_cycles_panel,
      dt_shp = cnty_shp_2020
    )
  ),
  tar_target(
    fhfa_hpi_lu_ml_zip5,
    f_get_fhfa_zip5_annual_lu_ml_panel(
      dt_hp = fhfa_annual_hp_zip5,
      file_path_lu = file_raw_lu_zip5_2020,
      dt_regional_pci_panel = bea_regional_pci_zip5_2020,
      dt_hm_cycles = housing_cycles_panel
    )
  ),
  tar_target(
    fhfa_hpi_lu_ml_zip3,
    f_get_fhfa_zip3_annual_lu_ml_panel(
      dt_hp = fhfa_annual_hp_zip3,
      file_path_lu = file_raw_lu_zip3,
      dt_regional_pci_panel = bea_regional_pci_zip3_2000,
      dt_hm_cycles = housing_cycles_panel
    )
  ),
  tar_target(
    fhfa_hpi_lu_ml_trct_panel,
    f_get_fhfa_tract_annual_lu_ml_panel(
      dt_hp = fhfa_annual_hp_trct,
      file_path_lu = file_raw_lu_trct,
      dt_regional_pci_panel = bea_regional_pci_tract_2020,
      dt_hm_cycles = housing_cycles_panel, 
      dt_shp = shp_trct_2020,
      dt_tract_cw = cw_trct_nhgis_trct
    )
  ),

  ## Zillow LU-ML panel
  tar_target(
    zillow_hpi_lu_ml_cbsa_panel,
    f_get_zillow_cbsa_zhvi_lu_ml_panel(
      dt_hp = zillow_hp_cbsa,
      file_path_lu = file_raw_lu_cbsa_2022,
      dt_regional_pci_panel = bea_regional_pci_cbsa_2022,
      dt_hm_cycles = housing_cycles_panel,
      dt_shp = cbsa_shp_2022 
    )
  ),
  tar_target(
    zillow_hpi_lu_ml_cnty_panel,
    f_get_zillow_county_zhvi_lu_ml_panel(
      dt_hp = zillow_hp_county,
      file_path_lu = file_raw_lu_cnty_2020,
      dt_regional_pci_panel = bea_regional_pci_county_2020,
      dt_hm_cycles = housing_cycles_panel,
      dt_shp = cnty_shp_2020
    )
  ),
  tar_target(
    zillow_hpi_lu_ml_zip5_panel,
    f_get_zillow_zip5_zhvi_lu_ml_panel(
      dt_hp = zillow_hp_zip,
      file_path_lu = file_raw_lu_zip5_2020,
      dt_regional_pci_panel = bea_regional_pci_zip5_2020,
      dt_hm_cycles = housing_cycles_panel
    )
  ),

  ## FHFA Quarterly zip3 LU-ML panel
  tar_target(
    fhfa_qtrly_hpi_lu_ml_zip3,
    f_get_fhfa_zip3_qtrly_lu_ml_panel(
      dt_hp = fhfa_hpi_qtr_zip3,
      file_path_lu = file_raw_lu_zip3,
      dt_regional_pci_panel = bea_regional_pci_zip3_2000,
      dt_hm_cycles = housing_cycles_panel
    )
  ),

  ## Mian and Sufi (2014) LU-ML
  tar_target(
    dt_mian_sufi_lu_ml,
    f_get_mian_sufi_lu_ml(
      file_path_ms = file_raw_mian_sufi_2014,
      file_path_lu = file_raw_lu_cnty_2010,
      dt_regional_pci_panel = bea_regional_pci_county_2010
    )
  )
)

