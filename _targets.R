## ./_targets.R

library(targets); library(tarchetypes)

tar_option_set(
  format = "parquet"
)

tar_source("core")

targets_files <- list.files("targets", pattern = ".R$", full.names = TRUE,
                            recursive = TRUE)
lapply(targets_files, source)

c(utils_targets,
  shp_targets, replication_data, fred_targets, hp_indices_targets,
  housing_unit_targets, 
  bea_targets, gmaps_targets, lu_ml_targets, bsh_targets, saiz_cw_targets,

  ## -- Analysis -- ##
  analysis_targets, 
  ## Replication analysis
  replication_mian_sufi_2014_targets, replication_gurenetal_2021_targets,
  replication_chaneyetal_2012_targets, replication_stroebelvavra_2019_targets,
  ## Gmaps-HP regressions
  gmps_hp_regs_targets, 
  ## Stage1 SC Panel Regs
  stage1_sc_panel_targets,
  ## Income elasticity
  income_elasticity_targets
  )
