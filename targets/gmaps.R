## targets/gmaps.R

shps_to_get_region_gmaps_amenities <- rbind(
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

gmaps_targets <- list(
  tar_target(
    file_raw_gmaps_amenity_demand_cbsa2023,
    here::here(
      "data-raw/gmaps-natural-amenities-2025-main/cbsa_amenity_demand_index_2023.parquet"
    ), 
    format = "file"
  ),

  tar_target(
    gmaps_amenity_shp,
    f_get_gmaps_amenities_with_shp(
      file_raw_gmaps_amenity_demand_cbsa2023 = file_raw_gmaps_amenity_demand_cbsa2023, 
      cbsa_shp_2023 = cbsa_shp_2023
    ),
    format = "rds"
  ), 

  ## RegionalGMaps Natural Amenities (uses CBSAs or nearest CBSA)
  tarchetypes::tar_map(
    values = shps_to_get_region_gmaps_amenities,
    names = c("geog_level", "geog_yr"),
    tar_target(
      gmaps_region_amenity_demand,
      f_get_regional_gmaps_amenity_demand(
        dt_gmaps_amenity_demand = gmaps_amenity_shp,
        target_shp = shp_sym,
        geog_level = geog_level,   
        geog_yr = geog_yr       
      )
    )
  )
)
