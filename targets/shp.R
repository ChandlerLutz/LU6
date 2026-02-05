## targets/shp.R

box::use(
  magrittr[`%>%`], sf[st_set_crs, st_transform],
  data.table[...]
)

shp_targets <- list(
  tar_target(nhgis_shp_metadata, f_get_nhgis_shp_metadata(), format = "parquet"),

  ## Raw shapefiles
  tar_target(
    shp_zip3_2000,
    readRDS(here::here("data-raw/census/shp/35-zip3_2000_shapefile.rds")) %>%
      st_set_crs(4269) %>% st_transform(crs = 5070) %>% as.data.table() %>%
      .[, GISJOIN := paste0("G", GEOID)], 
    format = "rds"
  ),
  tar_target(shp_zip5_2020,
             CLmisc::get_rnhgis_shp("us_zcta_2020_tl2020") %>%
               st_transform(crs = 4269) %>% 
               as.data.table() %>% .[, GEOID := GEOID20], 
             format = "rds"), 
  tar_target(shp_trct_1980,
             CLmisc::get_rnhgis_shp("us_tract_1980_tl2008") %>%
               as.data.table(),
             format = "rds"),
  tar_target(shp_trct_1990,
             CLmisc::get_rnhgis_shp("us_tract_1990_tl2008") %>%
               as.data.table(),
             format = "rds"),
  tar_target(shp_trct_2000,
             CLmisc::get_rnhgis_shp("us_tract_2000_tl2010") %>%
               as.data.table(),
             format = "rds"),
  tar_target(shp_trct_2010,
             CLmisc::get_rnhgis_shp("us_tract_2010_tl2020") %>%
               as.data.table(),
             format = "rds"),
  tar_target(shp_trct_2020,
             CLmisc::get_rnhgis_shp("us_tract_2020_tl2020") %>%
               as.data.table(),
             format = "rds"),

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
    raw_msa_shp_1999_file, here::here("data-raw/shp/10-saiz1999msa_shapefile.rds"), 
    format = "file"
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
        file_path_1999 = raw_msa_shp_1999_file, file_path_2007 = raw_cbsa_shp_2007_file
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
  tar_target(cw_cnty2020_cz2020,
             f_cw_geo_to_cz20(cnty_shp_2020, cz20_shp, id_col = "GEOID")),

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
