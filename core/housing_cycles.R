## core/housing_cycles.R


f_get_housing_cycles_panel <- function() {
  
  box::use(
    data.table[...], magrittr[`%>%`]
  )
  
  dt_housing_cycles_panel <- data.table(
    cycle_start_yr = c(1970, 1980, 1990, 2000, 2008, 2013, 2020),
    cycle_end_yr = c(1979, 1989, 1999, 2007, 2012, 2019, 2029),
    cycle_label = c(
      "1970s", "1980s", "1990s", "2000_2007", "2008_2012", "2013_2019", "2020s"
    )
  ) %>%
    .[, let(cycle_start_date = as.Date(sprintf("%d-01-01", cycle_start_yr)),
            cycle_end_date = as.Date(sprintf("%d-12-31", cycle_end_yr)))] %>%
    setcolorder(c("cycle_start_yr", "cycle_start_date", "cycle_end_yr",
                  "cycle_end_date"))
                  
  return(dt_housing_cycles_panel)
}

f_get_housing_cycles_decadal_panel <- function() {

  box::use(
    data.table[...], magrittr[`%>%`]    
  )

  dt_housing_cycles_decadal_panel <- data.table(
    cycle_start_yr = c(1970, 1980, 1990, 2000, 2010, 2020),
    cycle_end_yr = c(1979, 1989, 1999, 2009, 2019, 2029),
    cycle_label = c("1970s", "1980s", "1990s", "2000s", "2010s", "2020s")
  ) %>%
    .[, let(cycle_start_date = as.Date(sprintf("%d-01-01", cycle_start_yr)),
            cycle_end_date = as.Date(sprintf("%d-12-31", cycle_end_yr)))] %>%
    setcolorder(c("cycle_start_yr", "cycle_start_date", "cycle_end_yr",
                  "cycle_end_date"))

  return(dt_housing_cycles_decadal_panel)
}

