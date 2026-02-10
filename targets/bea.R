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
    file_raw_bea_cnty2020,
    here::here("data-raw/bea-constant-geographies/output/bea_in_cnty2020.rds"),
    format = "file"
  ),

  tar_target(
    file_raw_bea_cbsa2023,
    here::here("data-raw/bea-constant-geographies/output/bea_in_cbsa2023.rds"),
    format = "file"
  ),
    
  ## Regional BEA Income Data (uses CBSAs or Counties as a fallback
  tarchetypes::tar_map(
    values = shps_to_get_region_pci,
    names = c("geog_level", "geog_yr"),
    tar_target(
      bea_regional_pci,
      f_get_regional_bea_income(
        target_shp            = shp_sym,
        cbsa_shp_2023         = cbsa_shp_2023, 
        cnty_shp_2020         = cnty_shp_2020,
        file_raw_bea_cbsa     = file_raw_bea_cbsa2023,
        file_raw_bea_cnty     = file_raw_bea_cnty2020, 
        geog_level            = geog_level,   
        geog_yr               = geog_yr       
      )
    )
  )

  
)

