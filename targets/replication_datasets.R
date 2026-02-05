## targets/replication_datasets.R

box::use(
  data.table[...], magrittr[`%>%`]
)

replication_data <- list(

  tar_target(
    file_raw_mian_sufi_2014,
    here::here(
      "data-raw/MianSufi-econometrica/RdsFiles/10-mian_sufi_county_networth.rds"
    ),
    format = "file"
  )
)
