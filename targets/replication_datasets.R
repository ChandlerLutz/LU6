## targets/replication_datasets.R

box::use(
  data.table[...], magrittr[`%>%`]
)

replication_data <- list(

  ## Mian and Sufi (2014)
  tar_target(
    file_raw_mian_sufi_2014,
    here::here(
      "data-raw/MianSufi-econometrica/RdsFiles/10-mian_sufi_county_networth.rds"
    ),
    format = "file"
  ),

  ## Guren et al. (2021)
  tar_target(
    file_guren_replicate_fst,    here::here("data-raw/guren-et-al-replications-files/010-DT_replicate_guren_tab1_up_to_do_file_line213.fst"),
    format = "file"
  ),
  
  ## Stroebel and Vavra (2019) 
  tar_target(
    file_raw_sv_01_06,
    here::here(
      "data-raw/StroebelVavra2019/020-dt_StroebelVavra2019_msa_01_06_long_diff.parquet"
    ),
    format = "file"
  ),
  tar_target(
    file_raw_sv_07_11,
    here::here(
      "data-raw/StroebelVavra2019/020-dt_StroebelVavra2019_msa_07_11_long_diff.parquet"
    ),
    format = "file"
  ),

  ## Chaney et al. (2012)
  tar_target(
    file_raw_chaney_2012,
    here::here("data-raw/ChaneyEtAl2012/010-dt_chaneyetal_2012.parquet"),
    format = "file"
  )
)
