## core/saiz_utils.R

f_validate_shp_for_saiz_cw <- function(shp, expected_geog, year) {
  if (!inherits(shp, c("sf", "data.frame"))) {
    stop("Input must be an sf or data.frame object.")
  }
  
  expected_cols <- switch(expected_geog,
    "tract" = switch(as.character(year),
                     "2000" = c("GISJOIN", "CTIDFP00"),
                     "2010" = c("GISJOIN", "GEOID10"),
                     "2020" = c("GISJOIN", "GEOID"),
                     stop("Invalid year for tract. Use 2000, 2010, or 2020.")),
    "zip" = switch(as.character(year),
                   "2000" = "ZCTA5CE00",
                   "2010" = "GEOID10",
                   "2020" = "GEOID20",
                   stop("Invalid year for zip. Use 2000, 2010, or 2020.")),
    "cbsa" = "GEOID",
    stop("Unknown geography level passed to validation.")
  )
  
  missing_cols <- setdiff(expected_cols, names(shp))
  if (length(missing_cols) > 0) {
    stop(sprintf("Validation failed for %s %s. Missing columns: %s", 
                 expected_geog, year, paste(missing_cols, collapse = ", ")))
  }
  
  if (inherits(shp, "sf") && sf::st_crs(shp)$epsg != 5070) {
    warning("Input shapefile is not in EPSG:5070. Transforming now.")
    shp <- sf::st_transform(shp, 5070)
  }
  
  return(shp)
}

f_cw_saiz_to_tract <- function(dt_saiz, tract_shp, year) {
  box::use(data.table[...], magrittr[`%>%`])
  
  tract_shp <- f_validate_shp_for_saiz_cw(tract_shp, "tract", year)
  
  id_col <- switch(as.character(year),
                   "2000" = "CTIDFP00", "2010" = "GEOID10", "2020" = "GEOID")
  
  dt_trct_shp <- tract_shp %>%
    as.data.table() %>%
    setnames(id_col, "census_tract") %>%
    .[, census_trct_yr := year]
  
  dt_saiz_trct <- sf::st_join(
    x = sf::st_as_sf(dt_trct_shp),
    y = sf::st_as_sf(dt_saiz[, .(GEOID_saiz = GEOID, saiz_elasticity = elasticity,
                                 geometry)]),
    join = sf::intersects, left = FALSE, largest = TRUE
  ) %>% 
    as.data.table() %>%
    .[, .(saiz_elasticity = mean(saiz_elasticity, na.rm = TRUE)),
      keyby = .(GISJOIN, census_tract, census_trct_yr)]
  
  return(dt_saiz_trct)
}

f_cw_saiz_to_zip5 <- function(dt_saiz, zip_shp, year) {
  box::use(data.table[...], magrittr[`%>%`])
  
  zip_shp <- f_validate_shp_for_saiz_cw(zip_shp, "zip", year)
  
  id_col <- switch(as.character(year),
                   "2000" = "ZCTA5CE00", "2010" = "GEOID10", "2020" = "GEOID20")
  
  dt_zip5_shp <- zip_shp %>%
    as.data.table() %>%
    setnames(id_col, "zip5") %>%
    .[, zip5_yr := year]
  
  dt_saiz_zip5 <- sf::st_join(
    x = sf::st_as_sf(dt_zip5_shp),
    y = sf::st_as_sf(dt_saiz[, .(GEOID_saiz = GEOID,
                                 saiz_elasticity = elasticity, geometry)]),
    join = sf::intersects, left = FALSE, largest = TRUE
  ) %>% 
    as.data.table() %>%
    .[, .(saiz_elasticity = mean(saiz_elasticity, na.rm = TRUE)),
      keyby = .(zip5, zip5_yr)]
  
  return(dt_saiz_zip5)
}

f_cw_saiz_to_cbsa <- function(dt_saiz, cbsa_shp) {
  box::use(data.table[...], magrittr[`%>%`], CLmisc[select_by_ref])
  
  cbsa_shp <- f_validate_shp_for_saiz_cw(cbsa_shp, "cbsa", year = NA) 
  
  dt_saiz_cbsa <- sf::st_join(
    x = sf::st_as_sf(cbsa_shp[, .(GEOID_to = GEOID, geometry)]),
    y = sf::st_as_sf(dt_saiz[, .(GEOID_saiz = GEOID,
                                 saiz_elasticity = elasticity, geometry)]),
    join = sf::st_intersects, left = FALSE
  ) %>% 
    as.data.table() %>%
    .[, geometry := NULL] %>%
    merge(dt_saiz[, .(GEOID_saiz = GEOID, saiz_geom = geometry)], by = "GEOID_saiz") %>%
    merge(cbsa_shp[, .(GEOID_to = GEOID, cbsa_geom = geometry)], by = "GEOID_to") %>%
    .[, intersection_size := sf::st_area(sf::st_intersection(saiz_geom, cbsa_geom)),
      by = .I] %>%
    .[, is_largest := intersection_size == max(intersection_size),
      by = .(GEOID_saiz)] %>%
    .[is_largest == TRUE] %>%
    select_by_ref(c("GEOID_to", "saiz_elasticity")) %>%
    setnames("GEOID_to", "cbsa") %>%
    .[, .(saiz_elasticity = mean(saiz_elasticity, na.rm = TRUE)), by = cbsa]
  
  return(dt_saiz_cbsa)
}

