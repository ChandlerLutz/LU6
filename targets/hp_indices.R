## targets/hp_indices.R

hp_indices_targets <- list(

  ## FMCC HP 
  tar_target(
    file_raw_fmcc_hp, here::here("data-raw/fmcc/fmhpi_master_file_dwnld_202503.parquet"),
    format = "file"
  ),
  tar_target(
    fmcc_natl_hp, f_get_fmcc_natl_hp(file_path_fmcc = file_raw_fmcc_hp),
    format = "parquet"
  ),
  tar_target(
    fmcc_cbsa_hp,
    f_get_fmcc_cbsa_hp(
      file_path_fmcc = file_raw_fmcc_hp, dt_cz20 = cz20_shp, dt_cbsa_shp = cbsa_shp_2022
    ),
    format = "parquet"
  ), 

  ## FHFA Annual HPI
  tar_target(file_raw_fhfa_annual_zip3,
             here::here("data-raw/fhfa/annual_hpi/hpi_at_zip3_dwnld_202506.xlsx"),
             format="file"),
  tar_target(file_raw_fhfa_annual_zip5,
             here::here("data-raw/fhfa/annual_hpi/hpi_at_zip5_dwnld_202506.xlsx"),
             format="file"),
  tar_target(file_raw_fhfa_annual_cbsa,
             here::here("data-raw/fhfa/annual_hpi/hpi_at_cbsa_dwnld_202506.xlsx"),
             format="file"),
  tar_target(file_raw_fhfa_annual_cnty,
             here::here("data-raw/fhfa/annual_hpi/hpi_at_county_dwnld_202506.xlsx"),
             format="file"),
  tar_target(file_raw_fhfa_annual_trct,
             here::here("data-raw/fhfa/annual_hpi/hpi_at_tract_dwnld_202506.csv"),
             format="file"),
  tar_target(file_raw_fhfa_annual_natl,
             here::here("data-raw/fhfa/annual_hpi/hpi_at_national_dwnld_202506.xlsx"),
             format="file"),
  tar_target(fhfa_annual_hp_zip3, 
             f_get_fhfa_annual_hpi("zip3", file_raw_fhfa_annual_zip3,
                                   cw_zip3_2000_cz2020)),
  tar_target(fhfa_annual_hp_zip5, 
             f_get_fhfa_annual_hpi("zip5", file_raw_fhfa_annual_zip5,
                                   cw_zip5_2020_cz2020)),
  tar_target(fhfa_annual_hp_cbsa, 
             f_get_fhfa_annual_hpi("cbsa", file_raw_fhfa_annual_cbsa,
                                   cw_cbsa2020_cz2020)),
  
  tar_target(fhfa_annual_hp_cnty, 
             f_get_fhfa_annual_hpi("county", file_raw_fhfa_annual_cnty,
                                   cw_cnty2020_cz2020)),
  tar_target(fhfa_annual_hp_trct, 
             f_get_fhfa_annual_hpi("tract", file_raw_fhfa_annual_trct,
                                   cw_trct2020_cz2020)),
  tar_target(fhfa_annual_hp_natl, 
             f_get_fhfa_annual_hpi("natl", file_raw_fhfa_annual_natl)),

  ## FHFA Quarterly all-transactions HPI
  tar_target(
    file_raw_fhfa_at_qtr_zip3,
    here::here("data-raw/fhfa/quarterly_hpi/hpi_at_3zip_dwnld_202507.xlsx"),
    format = "file"
  ),
  tar_target(
    file_raw_fhfa_at_qtr_natl,
    here::here("data-raw/fhfa/quarterly_hpi/hpi_at_us_and_divisions_dwnld_202507.csv"),
    format = "file"
  ),
  tar_target(
    fhfa_hpi_qtr_zip3,
    f_get_fhfa_qtrly_at_hpi(
      geog_level = "zip3", file_path_raw = file_raw_fhfa_at_qtr_zip3,
      dt_to_cz20_cw = cw_zip3_2000_cz2020 
    )
  ),
  tar_target(
    fhfa_hpi_qtr_natl,
    f_get_fhfa_qtrly_at_hpi(
      geog_level = "natl", file_path_raw = file_raw_fhfa_at_qtr_natl
    )
  ),

  ## Zillow HPI
  tar_target(
    file_raw_zillow_cbsa,
    here::here("data-raw/zillow/zhvi_all/metro_zhvi_dwnld_20250319.parquet"),
    format = "file"
  ),
  tar_target(
    file_raw_zillow_county,
    here::here("data-raw/zillow/zhvi_all/county_zhvi_dwnld_20250319.parquet"),
    format = "file"
  ),
  tar_target(
    file_raw_zillow_zip,
    here::here("data-raw/zillow/zhvi_all/zip_zhvi_dwnld_20250319.parquet"),
    format = "file"
  ),

  tar_target(
    zillow_natl_hp, f_get_zillow_hp_natl(file_raw_zillow_cbsa)
  ),
  tar_target(
    zillow_hp_cbsa,
    f_get_zillow_hp_cbsa(
      file_path_metro = file_raw_zillow_cbsa, file_path_county = file_raw_zillow_county,
      cbsa_shp = cbsa_shp_2022, cz20_shp = cz20_shp
    )
  ),
  tar_target(
    zillow_hp_county, f_get_zillow_hp_county(file_raw_zillow_county, cw_cnty2020_cz2020)
  ),
  tar_target(zillow_hp_zip, f_get_zillow_hp_zip(file_raw_zillow_zip,
                                                cw_zip5_2020_cz2020))
)
