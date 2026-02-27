

box::use(
  magrittr[`%>%`], sf[st_set_crs, st_transform, st_simplify, st_cast],
  data.table[...]
)

ipums_key <- keyring::key_get("ipums_api_key")
ipumsr::set_ipums_api_key(ipums_key, save = FALSE)

shp_targets <- list(
  tar_target(nhgis_shp_metadata, f_get_nhgis_shp_metadata(), format = "parquet"),

  ## Raw shapefiles
  tar_target(
    file_raw_shp_zip3_2000,
    here::here("data-raw/census/shp/35-zip3_2000_shapefile.rds"),
    format = "file"
  ),
  tar_target(
    shp_zip3_2000,
    readRDS(file_raw_shp_zip3_2000) %>%
      st_set_crs(4269) %>% st_transform(crs = 5070) %>% as.data.table() %>%
      .[, GISJOIN := paste0("G", GEOID)], 
    format = "rds"
  ),
  tar_target(shp_states,
             CLmisc::get_rnhgis_shp("us_state_2023_tl2023") %>%
               st_transform(crs = 5070) %>% st_simplify(dTolerance = 1e03) %>%
               st_cast("MULTIPOLYGON") %>%
               as.data.table(), 
             format = "rds"),
  tar_target(shp_census_divisions,
             f_get_census_divisions_shp(
               shp_states = shp_states,
               file_path_census_regions_divisions = file_raw_census_st_regions_div
             ), 
             format = "rds"),
  tar_target(shp_zip5_2020,
             CLmisc::get_rnhgis_shp("us_zcta_2020_tl2020") %>%
               st_transform(crs = 5070) %>% 
               as.data.table() %>% .[, GEOID := GEOID20], 
             format = "rds"), 
  tar_target(shp_trct_1980,
             CLmisc::get_rnhgis_shp("us_tract_1980_tl2008") %>%
               st_transform(crs = 5070) %>%
               as.data.table(),
             format = "rds"),
  tar_target(shp_trct_1990,
             CLmisc::get_rnhgis_shp("us_tract_1990_tl2008") %>%
               st_transform(crs = 5070) %>%
               as.data.table(),
             format = "rds"),
  tar_target(shp_trct_2000,
             CLmisc::get_rnhgis_shp("us_tract_2000_tl2010") %>%
               st_transform(crs = 5070) %>%
               as.data.table() %>% .[, GEOID := CTIDFP00],
             format = "rds"),
  tar_target(shp_trct_2010,
             CLmisc::get_rnhgis_shp("us_tract_2010_tl2020") %>%
               st_transform(crs = 5070) %>%
               as.data.table() %>% .[, GEOID := GEOID10],
             format = "rds"),
  tar_target(shp_trct_2020,
             CLmisc::get_rnhgis_shp("us_tract_2020_tl2020") %>%
               st_transform(crs = 5070) %>%
               as.data.table(),
             format = "rds"),
  tar_target(shp_blkgrp_2010,
             CLmisc::get_rnhgis_shp("us_blck_grp_2010_tl2020") %>%
               st_transform(crs = 5070) %>%
               as.data.table() %>% .[, GEOID := GEOID10],
             format = "rds"),
    
  tar_target(
    file_raw_census_st_regions_div,
    here::here("data-raw/census/census_regions_divisions_states.csv"),
    format = "file"
  ), 

  ## county cleaned shps
  tarchetypes::tar_map(
    values = list(year = c(1980, 1990, 2000, 2010, 2015, 2020, 2023)),
    names = "year",
    tar_target(
      cnty_shp,
      f_get_cnty_shp(year = year, dt_nhgis_shp_metadata = nhgis_shp_metadata),
      format = "rds"
    )
  ), 

  ## CZ20 shp
  tar_target(
    file_raw_cnty23_cz20_cw,
    here::here("data-raw/usda/preliminary-2020-commuting-zones.csv"),
    format = "file"
  ), 
  tar_target(
    cz20_shp,
    f_get_cz20_shp(
      file_path_cnty23_cz20_cw = file_raw_cnty23_cz20_cw,
      dt_cnty_shp = cnty_shp_2023
    ),
    format = "rds"
  ),

  ## MSA/CBSA shp
  tar_target(
    raw_msa_shp_1999_file,
    here::here("data-raw/shp/10-saiz1999msa_shapefile.rds"), 
    format = "file"
  ),
  tar_target(
    msa_shp_1999,
    readRDS(raw_msa_shp_1999_file) %>%
      st_set_crs(4269) %>% st_transform(crs = 5070) %>% as.data.table() %>%
      .[, GEOID := as.character(GEOID)],
    format = "rds"
  ), 
  tar_target(
    raw_cbsa_shp_2007_file, here::here("data-raw/shp/cbsa2007_shp.rds"), format = "file"
  ), 
  tarchetypes::tar_map(
    values = list(year = c(1999, 2000, 2007, 2009, 2010, 2013:2023)),
    names = "year",
    tar_target(
      cbsa_shp,
      f_get_msa_cbsa_shp(
        year = year,dt_nhgis_shp_metadata = nhgis_shp_metadata,
        file_path_1999 = raw_msa_shp_1999_file,
        file_path_2007 = raw_cbsa_shp_2007_file
      ),
      format = "rds"
    )
  ),

  ## Crosswalks to CZ20
  tar_target(cw_zip3_2000_cz2020,
             f_cw_geo_to_cz20(shp_zip3_2000, cz20_shp, id_col = "GEOID")),
  tar_target(cw_zip5_2020_cz2020, f_cw_geo_to_cz20(shp_zip5_2020, cz20_shp,
                                                   id_col = "GEOID")),
  tar_target(cw_trct2020_cz2020, 
             f_cw_geo_to_cz20(shp_trct_2020, cz20_shp, id_col = "GEOID")),
  tar_target(cw_cbsa2020_cz2020,
             f_cw_geo_to_cz20(cbsa_shp_2020, cz20_shp, id_col = "GEOID")),
  tar_target(cw_cbsa2022_cz2020,
             f_cw_geo_to_cz20(cbsa_shp_2022, cz20_shp, id_col = "GEOID")),
  tar_target(cw_cbsa2023_cz2020,
             f_cw_geo_to_cz20(cbsa_shp_2023, cz20_shp, id_col = "GEOID")),
  tar_target(cw_cnty2020_cz2020,
             f_cw_geo_to_cz20(cnty_shp_2020, cz20_shp, id_col = "GEOID")),

  ## Crosswalks to States
  tar_target(cw_zip3_2000_state, f_cw_geo_to_state(shp_zip3_2000, shp_states,
                                                   id_col = "GEOID")),
  tar_target(cw_zip5_2020_state, f_cw_geo_to_state(shp_zip5_2020, shp_states,
                                                   id_col = "GEOID")),
  tar_target(cw_trct_2020_state, f_cw_geo_to_state(shp_trct_2020, shp_states,
                                                   id_col = "GEOID")),
  tar_target(cw_cbsa_2020_state, f_cw_geo_to_state(cbsa_shp_2020, shp_states,
                                                   id_col = "GEOID")),
  tar_target(cw_cbsa_2022_state, f_cw_geo_to_state(cbsa_shp_2022, shp_states,
                                                   id_col = "GEOID")),
  tar_target(cw_cbsa_2023_state, f_cw_geo_to_state(cbsa_shp_2023, shp_states,
                                                   id_col = "GEOID")),
  tar_target(cw_cnty_2020_state, f_cw_geo_to_state(cnty_shp_2020, shp_states,
                                                   id_col = "GEOID")),

  ## Crosswalk to Census Divisions
  tar_target(dt_division_zip3_2000,
             f_cw_to_census_divisions(shp_zip3_2000, shp_census_divisions)),
  tar_target(dt_division_zip5_2020, 
             f_cw_to_census_divisions(shp_zip5_2020, shp_census_divisions)), 
  tar_target(dt_division_trct_2020, 
             f_cw_to_census_divisions(shp_trct_2020, shp_census_divisions)), 
  tar_target(dt_division_cbsa_2020,
             f_cw_to_census_divisions(cbsa_shp_2020, shp_census_divisions)), 
  tar_target(dt_division_cbsa_2022,
             f_cw_to_census_divisions(cbsa_shp_2023, shp_census_divisions)),
  tar_target(dt_division_cbsa_2023,
             f_cw_to_census_divisions(cbsa_shp_2023, shp_census_divisions)),
  tar_target(dt_division_cnty_2020, 
             f_cw_to_census_divisions(cnty_shp_2020, shp_census_divisions)),

  ## Tract to NHGIS tract crosswalk
  tar_target(
    raw_nhgis_cw_tract,
    CLmisc::get_rnhgis_tst(name = "AV0", geog_levels = "tract") %>%
      as.data.table() %>% .[, names(.SD) := NULL, .SDcols = patterns("^NAME|^AV0")]
  ),
  tar_target(
    cw_trct_nhgis_trct,
    f_get_tract_to_nhgis_tract_cw(
      dt_nhgis_cw_raw   = raw_nhgis_cw_tract,
      shp_trct_1980 = shp_trct_1980,
      shp_trct_1990 = shp_trct_1990,
      shp_trct_2000 = shp_trct_2000,
      shp_trct_2010 = shp_trct_2010,
      shp_trct_2020 = shp_trct_2020
    )
  )
  
)
