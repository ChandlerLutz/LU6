## core/bsh_utils.R

f_crosswalk_baum_snow_han <- function(dt_bsh, dt_cw, geog_level) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref]
  )
  
  dt_bsh_clean <- copy(dt_bsh) %>%
    select_by_ref(c("ctracts2000", "gamma01b_space_FMM")) %>%
    .[, .(gamma01b_space_FMM = mean(gamma01b_space_FMM)), keyby = c("ctracts2000")]
  
  if (geog_level == "tract2020") {
    
    dt_cw_clean <- copy(dt_cw) %>%
      select_by_ref(c("from_census_tract", "to_gjoin", "to_census_tract",
                      "housing_units_2000")) %>%
      setnames("from_census_tract", "ctracts2000") %>%
      setnames("to_census_tract", "GEOID") %>%
      setnames("to_gjoin", "GISJOIN") %>%
      setnames("housing_units_2000", "hu_trct00")
    
    dt_out <- merge(dt_cw_clean, dt_bsh_clean, by = "ctracts2000") %>%
      .[, afact := hu_trct00 / sum(hu_trct00), by = .(GISJOIN, GEOID)] %>%
      .[, .(gamma01b_space_FMM = sum(gamma01b_space_FMM * afact)),
        keyby = .(GISJOIN, GEOID)] %>%
      .[!is.na(GISJOIN)]
    
    if (anyDuplicated(dt_out$GEOID)) stop("Duplicate Tracts found in BSH Crosswalk")
    
  } else if (geog_level == "zip5_2020") {
    
    dt_cw_clean <- copy(dt_cw) %>%
      select_by_ref(c("census_tract", "zip5_2020", "housing_units")) %>%
      setnames("census_tract", "ctracts2000") %>% 
      setnames("housing_units", "hu_trct00")
    
    dt_out <- merge(dt_cw_clean, dt_bsh_clean, by = "ctracts2000") %>%
      .[, afact := hu_trct00 / sum(hu_trct00), by = zip5_2020] %>%
      .[, .(gamma01b_space_FMM = sum(gamma01b_space_FMM * afact)),
        keyby = zip5_2020] %>%
      .[!is.na(zip5_2020)] %>%
      setnames("zip5_2020", "GEOID")
    
    if (anyDuplicated(dt_out$GEOID)) stop("Duplicate Zips found in BSH Crosswalk")
    
  } else {
    stop("Invalid geog_level provided. Must be 'tract2020' or 'zip5_2020'.")
  }

  setcolorder(dt_out, "GEOID")
  return(dt_out)
}

f_aggregate_baum_snow_han <- function(dt_bsh, dt_shp, dt_trct_hu, sf_trct_shp, 
                                      geog_level, year) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref]
  )
  
  stopifnot(geog_level %chin% c("metro", "cnty", "zip3"))
  
  dt_bsh_clean <- copy(dt_bsh) %>%
    select_by_ref(c("ctracts2000", "gamma01b_space_FMM"))
  
  dt_trct_hu_clean <- copy(dt_trct_hu) %>%
    .[!is.na(GJOIN2000)] %>%
    select_by_ref(c("GJOIN2000", "A41AA2000")) %>%
    setnames("GJOIN2000", "gjoin_trct") %>%
    setnames("A41AA2000", "hu_trct00") %>%
    .[!is.na(hu_trct00)]
  
  sf_trct_clean <- copy(sf_trct_shp) %>%
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "CTIDFP00", "geometry")) %>%
    setnames("GISJOIN", "gjoin_trct") %>%
    setnames("CTIDFP00", "ctracts2000") %>%
    sf::st_as_sf() %>%
    sf::st_transform(5070)
  
  dt_shp_clean <- copy(dt_shp)
  if (!inherits(dt_shp_clean, "sf")) dt_shp_clean <- sf::st_as_sf(dt_shp_clean)
  
  dt_shp_clean <- sf::st_transform(dt_shp_clean, 5070) %>%
    as.data.table()

  dt_shp_geoid_col <- grep("^GEOID", names(dt_shp_clean), value = TRUE)[1]
  if (is.na(dt_shp_geoid_col) && "zip3" %in% names(dt_shp_clean)) dt_shp_geoid_col <- "zip3"
  
  stopifnot(!is.na(dt_shp_geoid_col))
  
  dt_shp_clean <- dt_shp_clean %>%
    setnames(dt_shp_geoid_col, "GEOID") 
  
  if (!"GISJOIN" %in% names(dt_shp_clean)) {
      dt_shp_clean[, GISJOIN := paste0("G", GEOID)]
  }

  dt_shp_clean <- dt_shp_clean %>%
    select_by_ref(c("GISJOIN", "GEOID", "geometry")) %>%
    .[, gjoin_geog_yr := year] %>%
    setcolorder(c("GISJOIN", "GEOID", "gjoin_geog_yr", "geometry"))
  
  gjoin_col_name <- paste0("gjoin_", geog_level)
  setnames(dt_shp_clean, c("GISJOIN", "gjoin_geog_yr"), c(gjoin_col_name, paste0(gjoin_col_name, "_yr")))

  dt_bsh_aggregated_out <- sf::st_join(
    x = sf_trct_clean,
    y = sf::st_as_sf(dt_shp_clean),
    join = sf::st_intersects,
    left = FALSE, largest = TRUE
  ) %>%
    as.data.table() %>%
    .[, geometry := NULL] %>%
    merge(dt_bsh_clean, by = "ctracts2000") %>%
    merge(dt_trct_hu_clean, by = "gjoin_trct") %>%
    .[!is.na(hu_trct00) & hu_trct00 > 0] %>% 
    .[, .(hu_trct00 = mean(hu_trct00),
          gamma01b_space_FMM = weighted.mean(gamma01b_space_FMM, hu_trct00)),
      by = .(gjoin_trct, GEOID, ..gjoin_var, ..gjoin_var_yr),
      env = list(
        ..gjoin_var = gjoin_col_name,
        ..gjoin_var_yr = paste0(gjoin_col_name, "_yr")
      )] %>%
    .[, .(gamma01b_space_FMM = sum(gamma01b_space_FMM * hu_trct00 / sum(hu_trct00))),
      by = .(GEOID, ..gjoin_var, ..gjoin_var_yr),
      env = list(
        ..gjoin_var = gjoin_col_name,
        ..gjoin_var_yr = paste0(gjoin_col_name, "_yr")
      )]
  
  setnames(dt_bsh_aggregated_out, "GEOID", paste0("GEOID_", geog_level))
  dt_bsh_aggregated_out[, GEOID := get(paste0("GEOID_", geog_level))]
  setcolorder(dt_bsh_aggregated_out, "GEOID")
  
  return(dt_bsh_aggregated_out)
}
