## core/saiz_utils.R

f_validate_shp_for_saiz_cw <- function(shp, expected_geog, year) {
  if (!inherits(shp, c("sf", "data.frame"))) {
    stop("Input must be an sf or data.frame object.")
  }
  
  expected_cols <- switch(expected_geog,
    "tract" = "GEOID", 
    "zip" = "GEOID", 
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
  
  box::use(data.table[...], magrittr[`%>%`], CLmisc[select_by_ref])
  
  tract_shp <- f_validate_shp_for_saiz_cw(tract_shp, "tract", year) %>%
    select_by_ref(c("GISJOIN", "GEOID", "geometry"))
    
  dt_trct_shp <- tract_shp %>% as.data.table() %>% copy() %>% 
    .[, census_trct_yr := year]
  
  dt_out <- sf::st_join(sf::st_as_sf(dt_trct_shp), 
                        sf::st_as_sf(dt_saiz[, .(GEOID_saiz = GEOID,
                                                 saiz_elasticity = elasticity,
                                                 geometry)]),
                        join = sf::intersects, left = FALSE, largest = TRUE) %>% 
    as.data.table() %>%
    .[, .(saiz_elasticity = mean(saiz_elasticity, na.rm = TRUE)),
      keyby = .(GISJOIN, GEOID, census_trct_yr)] %>%
    setcolorder(c("GISJOIN", "GEOID", "census_trct_yr", "saiz_elasticity")) %>%
    setorder("GEOID")
  
  return(dt_out)
}

f_cw_saiz_to_zip5 <- function(dt_saiz, zip_shp, year) {
  
  box::use(data.table[...], magrittr[`%>%`], CLmisc[select_by_ref])
  
  zip_shp <- f_validate_shp_for_saiz_cw(zip_shp, "zip", year)
    
  dt_zip5_shp <- zip_shp %>% as.data.table() %>% copy() %>% 
    select_by_ref(c("GISJOIN", "GEOID", "geometry"))
  
  dt_out <- sf::st_join(sf::st_as_sf(dt_zip5_shp),
                        sf::st_as_sf(dt_saiz[, .(GEOID_saiz = GEOID,
                                                 saiz_elasticity = elasticity,
                                                 geometry)]),
                        join = sf::intersects, left = FALSE, largest = TRUE) %>% 
    as.data.table() %>%
    .[, .(saiz_elasticity = mean(saiz_elasticity, na.rm = TRUE)),
      keyby = .(GISJOIN, GEOID)] %>%
    .[, zip5_yr := year] %>%
    setcolorder(c("GISJOIN", "GEOID", "zip5_yr", "saiz_elasticity")) %>%
    setorder("GEOID")
  

  return(dt_out)
}

f_cw_saiz_to_county <- function(dt_saiz, dt_cnty_shp, year) {
  
  box::use(
    data.table[...], magrittr[`%>%`], sf[st_as_sf, st_join, st_intersects]
  )
  
  stopifnot(
    all(c("GISJOIN", "GEOID", "geometry") %in% names(dt_cnty_shp)),
    "elasticity" %in% names(dt_saiz)
  )

  dt_out <- sf::st_join(sf::st_as_sf(dt_cnty_shp),
                        sf::st_as_sf(dt_saiz[, .(GEOID_saiz = GEOID,
                                                 saiz_elasticity = elasticity,
                                                 geometry)]),
                        join = sf::intersects, left = FALSE, largest = TRUE) %>% 
    as.data.table() %>%
    .[, .(saiz_elasticity = mean(saiz_elasticity, na.rm = TRUE)),
      keyby = .(GISJOIN, GEOID)]
  
  dt_out <- dt_out[, cnty_census_yr := year]
  
  setcolorder(dt_out, c("GEOID", "GISJOIN", "cnty_census_yr", "saiz_elasticity"))
  
  return(dt_out)
}

f_cw_saiz_to_zip3 <- function(dt_saiz, dt_zip3_shp, year) {
  
  box::use(
    data.table[...], magrittr[`%>%`], sf[st_as_sf, st_join, st_intersects]
  )
  
  stopifnot(
    all(c("GISJOIN", "GEOID", "geometry") %in% names(dt_zip3_shp)),
    "elasticity" %in% names(dt_saiz)
  )

  dt_out <- sf::st_join(sf::st_as_sf(dt_zip3_shp),
                        sf::st_as_sf(dt_saiz[, .(GEOID_saiz = GEOID,
                                                 saiz_elasticity = elasticity,
                                                 geometry)]),
                        join = sf::intersects, left = FALSE, largest = TRUE) %>% 
    as.data.table() %>%
    .[, .(saiz_elasticity = mean(saiz_elasticity, na.rm = TRUE)),
      keyby = .(GISJOIN, GEOID)]
  
  dt_out <- dt_out[, zip3_census_yr := year]
  
  setcolorder(dt_out, c("GEOID", "GISJOIN", "zip3_census_yr", "saiz_elasticity"))
  
  return(dt_out)
}



f_cw_saiz_to_cbsa <- function(dt_saiz, cbsa_shp) {
  box::use(data.table[...], magrittr[`%>%`], CLmisc[select_by_ref])
  cbsa_shp <- f_validate_shp_for_saiz_cw(cbsa_shp, "cbsa", year = NA) 
  
  dt_out <- sf::st_join(sf::st_as_sf(cbsa_shp[, .(GEOID_to = GEOID, geometry)]),
                        sf::st_as_sf(dt_saiz[, .(GEOID_saiz = GEOID,
                                                 saiz_elasticity = elasticity,
                                                 geometry)]),
                        join = sf::st_intersects, left = FALSE) %>% 
    as.data.table() %>% .[, geometry := NULL] %>%
    merge(dt_saiz[, .(GEOID_saiz = GEOID, saiz_geom = geometry)], by = "GEOID_saiz") %>%
    merge(cbsa_shp[, .(GEOID_to = GEOID, cbsa_geom = geometry)], by = "GEOID_to") %>%
    .[, intersection_size := sf::st_area(sf::st_intersection(saiz_geom, cbsa_geom)),
      by = .I] %>%
    .[, is_largest := intersection_size == max(intersection_size),
      by = .(GEOID_saiz)] %>%
    .[is_largest == TRUE] %>%
    .[, .(saiz_elasticity = mean(saiz_elasticity, na.rm = TRUE)),
      by = .(cbsa = GEOID_to)]
  
  dt_out[, GEOID := cbsa]
  setcolorder(dt_out, "GEOID")
  return(dt_out)
}
