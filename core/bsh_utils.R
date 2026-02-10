## core/bsh_utils.R

f_crosswalk_baum_snow_han <- function(dt_bsh, dt_cw, geog_level) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref]
  )
  
  # dt_bsh should already be filtered/selected by the target before passing in, 
  # or we do it here if it's the raw import
  dt_bsh_clean <- copy(dt_bsh) %>%
    select_by_ref(c("ctracts2000", "gamma01b_space_FMM")) %>%
    .[, .(gamma01b_space_FMM = mean(gamma01b_space_FMM)), keyby = c("ctracts2000")]
  
  if (geog_level == "tract2020") {
    
    # Expecting dt_cw to be the Tract 2000 -> Tract 2020 crosswalk
    dt_cw_clean <- copy(dt_cw) %>%
      select_by_ref(c("from_census_tract", "to_census_tract", "housing_units_2000")) %>%
      setnames("from_census_tract", "ctracts2000") %>%
      setnames("to_census_tract", "ctracts2020") %>%
      setnames("housing_units_2000", "hu_trct00")
    
    dt_out <- merge(dt_cw_clean, dt_bsh_clean, by = "ctracts2000") %>%
      .[, afact := hu_trct00 / sum(hu_trct00), by = ctracts2020] %>%
      .[, .(gamma01b_space_FMM = sum(gamma01b_space_FMM * afact)),
        keyby = ctracts2020] %>%
      .[!is.na(ctracts2020)]
    
    if (anyDuplicated(dt_out$ctracts2020)) stop("Duplicate Tracts found in BSH Crosswalk")
    
    return(dt_out)
    
  } else if (geog_level == "zip5_2020") {
    
    # Expecting dt_cw to be the Tract 2000 -> Zip5 2020 crosswalk
    dt_cw_clean <- copy(dt_cw) %>%
      select_by_ref(c("census_tract", "zip5_2020", "housing_units")) %>%
      setnames("census_tract", "ctracts2000") %>% 
      setnames("housing_units", "hu_trct00")
    
    dt_out <- merge(dt_cw_clean, dt_bsh_clean, by = "ctracts2000") %>%
      .[, afact := hu_trct00 / sum(hu_trct00), by = zip5_2020] %>%
      .[, .(gamma01b_space_FMM = sum(gamma01b_space_FMM * afact)),
        keyby = zip5_2020] %>%
      .[!is.na(zip5_2020)]
    
    if (anyDuplicated(dt_out$zip5_2020)) stop("Duplicate Zips found in BSH Crosswalk")
    
    return(dt_out)
    
  } else {
    stop("Invalid geog_level provided. Must be 'tract2020' or 'zip5_2020'.")
  }
}

f_aggregate_baum_snow_han <- function(dt_bsh, dt_shp, dt_trct_hu, sf_trct_shp, 
                                      geog_level, year) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref]
  )
  
  stopifnot(geog_level %chin% c("metro", "cnty", "zip3"))
  
  # Prepare Inputs
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
  
  # Prepare Shapefile (Target Shapefile)
  dt_shp_clean <- copy(dt_shp)
  
  # Handle specific geometry transformations if not already done in target
  # But ideally, we assume the input dt_shp is already an SF object or compatible
  if (!inherits(dt_shp_clean, "sf")) dt_shp_clean <- sf::st_as_sf(dt_shp_clean)
  
  dt_shp_clean <- sf::st_transform(dt_shp_clean, 5070) %>%
    as.data.table()

  # Identify GEOID column
  # We assume the standard naming convention from shp_utils or standard files
  dt_shp_geoid_col <- grep("^GEOID", names(dt_shp_clean), value = TRUE)[1]
  if (is.na(dt_shp_geoid_col) && "zip3" %in% names(dt_shp_clean)) dt_shp_geoid_col <- "zip3"
  
  stopifnot(!is.na(dt_shp_geoid_col))
  
  # Standardize Shapefile Columns for Aggregation
  dt_shp_clean <- dt_shp_clean %>%
    setnames(dt_shp_geoid_col, "GEOID") 
  
  # Handle GISJOIN if present, otherwise create dummy or use GEOID
  if (!"GISJOIN" %in% names(dt_shp_clean)) {
      dt_shp_clean[, GISJOIN := paste0("G", GEOID)]
  }

  dt_shp_clean <- dt_shp_clean %>%
    select_by_ref(c("GISJOIN", "GEOID", "geometry")) %>%
    .[, paste0("gjoin_", geog_level, "_yr") := year] %>%
    setcolorder(c("GISJOIN", "GEOID", paste0("gjoin_", geog_level, "_yr"), "geometry")) %>% 
    setnames("GISJOIN", paste0("gjoin_", geog_level)) %>% 
    setnames("GEOID", paste0("GEOID_", geog_level))
  
  # Spatial Join
  dt_bsh_aggregated_out <- sf::st_join(
    x = sf_trct_clean,
    y = sf::st_as_sf(dt_shp_clean),
    join = sf::st_covered_by,
    left = FALSE
  ) %>%
    as.data.table() %>%
    .[, geometry := NULL] %>%
    merge(dt_bsh_clean, by = "ctracts2000") %>%
    merge(dt_trct_hu_clean, by = "gjoin_trct") %>%
    .[!is.na(hu_trct00) & hu_trct00 > 0] %>% 
    .[, .(hu_trct00 = mean(hu_trct00),
          gamma01b_space_FMM = weighted.mean(gamma01b_space_FMM, hu_trct00)),
      by = .(gjoin_trct, ..gjoin_var, ..GEOID_var, ..gjoin_var_yr),
      env = list(
        ..gjoin_var = paste0("gjoin_", geog_level),
        ..gjoin_var_yr = paste0("gjoin_", geog_level, "_yr"),
        ..GEOID_var = paste0("GEOID_", geog_level)
      )] %>%
    .[, .(gamma01b_space_FMM = sum(gamma01b_space_FMM * hu_trct00 / sum(hu_trct00))),
      by = .(..gjoin_var, ..GEOID_var, ..gjoin_var_yr),
      env = list(
        ..gjoin_var = paste0("gjoin_", geog_level),
        ..gjoin_var_yr = paste0("gjoin_", geog_level, "_yr"),
        ..GEOID_var = paste0("GEOID_", geog_level)
      )]
  
  return(dt_bsh_aggregated_out)
}
