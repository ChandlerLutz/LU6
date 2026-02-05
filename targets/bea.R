# targets/bea.R

box::use(data.table[...], magrittr[`%>%`])

shps_to_get_region_pci <- rbind(
  data.table(geog_level = "cbsa",
             geog_yr = c(1999, 2000, 2007, 2009, 2010, 2013:2023)) %>%
    .[, shp_name := paste0("cbsa_shp_", geog_yr)],
  data.table(geog_level = "county",
             geog_yr = c(1980, 1990, 2000, 2010, 2015, 2020, 2023)) %>%
    .[, shp_name := paste0("cnty_shp_", geog_yr)],
  data.table(geog_level = "zip3", geog_yr = 2000, shp_name = "shp_zip3_2000"),
  data.table(geog_level = "zip5", geog_yr = 2020, shp_name = "shp_zip5_2020"),
  data.table(geog_level = "tract", geog_yr = 2020, shp_name = "shp_trct_2020")
) %>%
  .[, shp_sym := rlang::syms(shp_name)]


bea_targets <- list(
  
  tar_target(
    file_raw_bea_msa,
    here::here("data-raw/bea/CAINC1/BEA_CANC1_MSA_per_capita_income.csv"),
    format = "file"
  ),
  tar_target(
    file_raw_bea_micropolitan,
    here::here("data-raw/bea/CAINC1/BEA_CANC1_Micropolitan_Statistical_Area_per_capita_income.csv"),
    format = "file"
  ),
  tar_target(
    file_raw_bea_csa,
    here::here("data-raw/bea/CAINC1/BEA_CANC1_CSA_per_capita_income.csv"),
    format = "file"
  ),
  tar_target(
    file_raw_bea_div,
    here::here("data-raw/bea/CAINC1/BEA_CANC1_Metropolitan_Division_per_capita_income.csv"),
    format = "file"
  ),

  tar_target(
    bea_cbsa_pci,
    f_get_bea_cbsa_per_capita_income(
      file_msa = file_raw_bea_msa,
      file_micro = file_raw_bea_micropolitan,
      file_csa = file_raw_bea_csa,
      file_div = file_raw_bea_div
    )
  ),

  ## BEA Cainc1 files for States and Counties 
  tar_target(
    bea_cainc1_file_paths,
    {
      states <- sort(c(state.abb[!state.abb %in% c("AK", "HI")], "DC"))
      here::here(sprintf("data-raw/bea/CAINC1/CAINC1_cnty/CAINC1_%s_1969_2023.csv",
                         states))
    },
    format = "file"
  ),

  tar_target(
    bea_cainc1_raw,
    f_load_raw_bea_cainc1(bea_cainc1_file_paths)
  ),

  tar_target(
    bea_state_income,
    f_process_bea_state_income(bea_cainc1_raw)
  ),
  
  tar_target(
    bea_county_income,
    f_process_bea_county_income(
      dt_raw = bea_cainc1_raw,
      cnty_shp = cnty_shp_2020 
    )
  ), 
  
  ## Regional BEA Income Data (uses CBSAs or Counties as a fallback
  tarchetypes::tar_map(
    values = shps_to_get_region_pci,
    names = c("geog_level", "geog_yr"),
    tar_target(
      bea_regional_pci,
      f_get_regional_bea_income(
        target_shp      = shp_sym,
        cbsa_shp_2022   = cbsa_shp_2022, 
        cnty_shp_2020   = cnty_shp_2020, 
        dt_bea_cbsa_pci = bea_cbsa_pci,  
        dt_bea_cnty_pci = bea_county_income,
        geog_level      = geog_level,   
        geog_yr         = geog_yr       
      )
    )
  )

  
)

