## ./_targets.R

library(targets); library(tarchetypes)

tar_option_set(
  format = "parquet"
)

core_files <- list.files("core", pattern = ".R$", full.names = TRUE)
lapply(core_files, source)

targets_files <- list.files("targets", pattern = ".R$", full.names = TRUE,
                            recursive = TRUE)
lapply(targets_files, source)

c(shp_targets, replication_data, fred_targets, hp_indices_targets,
  bea_targets, lu_ml_targets, bsh_targets, saiz_cw_targets, 
  ## Replication analysis
  replication_mian_sufi_2014_targets, replication_gurenetal_2021_targets,
  replication_chaneyetal_2012_targets, replication_stroebelvavra_2019_targets
  )
