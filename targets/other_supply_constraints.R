## targets/other_supply_constraints.R

box::use(
  data.table[...], magrittr[`%>%`]
)

bsh_targets <- list(
  
  # --- Raw Files ---
  tar_target(
    file_raw_bsh_gammas,
    here::here("data-raw/baum-snow-han/gamma_estimates_sep2023/gammas_hat_all.dta"),
    format = "file"
  ),
  tar_target(
    file_raw_cw_trct00_trct20,
    here::here(
      "data-raw/census-tract-to-tract-crosswalks/cw_tract2000_to_tract2020.parquet"
    ),
    format = "file"
  ),
  tar_target(
    file_raw_cw_trct00_zip5_20,
    here::here(
      "data-raw/census-tract-to-zip5-2020-crosswalk/cw_trct2000_to_zip5_2020.parquet"
    ),
    format = "file"
  ),

  # --- Read Raw Data ---
  tar_target(
    dt_bsh_gammas_raw,
    haven::read_dta(file_raw_bsh_gammas) %>% as.data.table()
  ),
  tar_target(
    dt_cw_trct00_trct20,
    nanoparquet::read_parquet(file_raw_cw_trct00_trct20) %>% as.data.table(),
    format = "rds"
  ),
  tar_target(
    dt_cw_trct00_zip5_20,
    nanoparquet::read_parquet(file_raw_cw_trct00_zip5_20) %>% as.data.table(),
    format = "rds"
  ),
  
  # --- Census Inputs for Aggregation (NHGIS) ---
  # Assuming CLmisc works in the environment
  tar_target(
    dt_trct_hu_2000,
    CLmisc::get_rnhgis_tst(name = "A41", geog_levels = "tract") %>% setDT()
  ),
  tar_target(
    sf_trct_2000,
    CLmisc::get_rnhgis_shp("us_tract_2000_tl2010"),
    format = "rds"
  ),

  # --- Crosswalks (Tract 2020 & Zip 5 2020) ---
  tar_target(
    dt_bsh_trct2020,
    f_crosswalk_baum_snow_han(
      dt_bsh = dt_bsh_gammas_raw,
      dt_cw = dt_cw_trct00_trct20,
      geog_level = "tract2020"
    )
  ),
  tar_target(
    dt_bsh_zip5_2020,
    f_crosswalk_baum_snow_han(
      dt_bsh = dt_bsh_gammas_raw,
      dt_cw = dt_cw_trct00_zip5_20,
      geog_level = "zip5_2020"
    )
  ),

  # --- Aggregations (Metro, County, Zip3) ---
  
  # Metros/CBSAs (Uses cbsa_shp_2010 from targets/shp.R)
  tar_target(
    dt_bsh_metro_2010,
    f_aggregate_baum_snow_han(
      dt_bsh = dt_bsh_gammas_raw,
      dt_shp = cbsa_shp_2010, 
      dt_trct_hu = dt_trct_hu_2000,
      sf_trct_shp = sf_trct_2000,
      geog_level = "metro",
      year = 2010
    )
  ),
  tar_target(
    dt_bsh_cbsa_2015,
    f_aggregate_baum_snow_han(
      dt_bsh = dt_bsh_gammas_raw,
      dt_shp = cbsa_shp_2015, 
      dt_trct_hu = dt_trct_hu_2000,
      sf_trct_shp = sf_trct_2000,
      geog_level = "metro",
      year = 2015
    )
  ),
  
  # County 2010 (Uses cnty_shp_2010 from targets/shp.R)
  tar_target(
    dt_bsh_county_2010,
    f_aggregate_baum_snow_han(
      dt_bsh = dt_bsh_gammas_raw,
      dt_shp = cnty_shp_2010,
      dt_trct_hu = dt_trct_hu_2000,
      sf_trct_shp = sf_trct_2000,
      geog_level = "cnty",
      year = 2010
    )
  ),
  
  # Zip3 2000
  tar_target(
    dt_bsh_zip3_2000,
    f_aggregate_baum_snow_han(
      dt_bsh = dt_bsh_gammas_raw,
      dt_shp = shp_zip3_2000,
      dt_trct_hu = dt_trct_hu_2000,
      sf_trct_shp = sf_trct_2000,
      geog_level = "zip3",
      year = 2000
    )
  )
)
