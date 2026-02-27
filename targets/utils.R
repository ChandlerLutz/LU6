## targets/utils.R

utils_targets <- list(

  ## Housing Market Cycles
  tar_target(dt_housing_cycles_panel, f_get_housing_cycles_panel()), 
  tar_target(dt_housing_cycles_decadal_panel, f_get_housing_cycles_decadal_panel())
  
)
