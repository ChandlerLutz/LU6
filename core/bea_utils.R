# core/bea_utils.R

f_get_regional_bea_income <- function(target_shp, cbsa_shp_2023, cnty_shp_2020, 
                                      file_raw_bea_cbsa, file_raw_bea_cnty,
                                      geog_level, geog_yr) {
  
  box::use(
    data.table[...], magrittr[`%>%`], 
    sf[st_transform, st_join, st_intersects, st_as_sf], 
    CLmisc[select_by_ref, pipe_check]
  )

  # --- 1. Prepare Target Shape ---
  dt_target_shp <- copy(target_shp) %>% 
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "GEOID", "geometry")) %>%
    setcolorder(c("GISJOIN", "GEOID", "geometry")) %>%
    setnames("GISJOIN", "target_gjoin") %>%
    setnames("GEOID", "target_geoid") %>% 
    .[, geometry := st_transform(geometry, crs = 5070)]

  stopifnot(names(dt_target_shp) == c("target_gjoin", "target_geoid", "geometry"))

  # --- 2. Prepare Reference Shapes ---
  # Transform reference shapes to match CRS 5070
  
  dt_cbsa_ref <- copy(cbsa_shp_2023) %>%
    select_by_ref(c("GEOID", "geometry")) %>%
    setcolorder(c("GEOID", "geometry")) %>%
    .[, geometry := st_transform(geometry, crs = 5070)]

  dt_cnty_ref <- copy(cnty_shp_2020) %>%
    select_by_ref(c("GEOID", "geometry")) %>%
    setcolorder(c("GEOID", "geometry")) %>%
    .[, geometry := st_transform(geometry, crs = 5070)]
    
  # --- 3. Prepare Reference Income Data ---
  # Read RDS files and standardize columns to 'GEOID', 'year', 'regional_pci'
  
  dt_cbsa_pci <- readRDS(file_raw_bea_cbsa) %>%
    setDT() %>%
    .[GEOID %in% dt_cbsa_ref$GEOID] %>%
    select_by_ref(c("GEOID", "year", "PerCapitaIncome_Dollars")) %>%
    setnames("PerCapitaIncome_Dollars", "regional_pci") %>%
    setcolorder(c("GEOID", "year", "regional_pci"))

  dt_cnty_pci <- readRDS(file_raw_bea_cnty) %>%
    setDT() %>%
    select_by_ref(c("GEOID", "year", "PerCapitaIncome_Dollars")) %>%
    setnames("PerCapitaIncome_Dollars", "regional_pci") %>%
    setcolorder(c("GEOID", "year", "regional_pci"))

  # --- 4. Spatial Join: Target -> CBSA (Primary) ---
  # Join largest=TRUE ensures we match to the CBSA covering the most area
  dt_regional_pci <- st_join(
    st_as_sf(dt_target_shp),
    st_as_sf(dt_cbsa_ref),
    join = st_intersects,
    left = FALSE, largest = TRUE
  ) %>%
    as.data.table() %>%
    .[, geometry := NULL] %>%
    pipe_check(check = nrow(.[duplicated(target_gjoin)]) == 0) %>%
    merge(dt_cbsa_pci, by = "GEOID", allow.cartesian = TRUE) %>%
    .[, `:=`(
        geog_level = geog_level,
        geog_yr = geog_yr,
        region_geog_level = "cbsa",
        region_geog_yr = 2023  
    )] %>%
    setnames("GEOID", "region_geoid")

  # --- 5. Spatial Join: Leftovers -> County (Fallback) ---
  # Identify target geometries that did not match a CBSA (or had no income data there)
  target_shp_leftover <- dt_target_shp %>%
    .[target_gjoin %notin% unique(dt_regional_pci$target_gjoin)]

  if (nrow(target_shp_leftover) > 0) {

    dt_regional_pci_cnty <- st_join(
      st_as_sf(target_shp_leftover),
      st_as_sf(dt_cnty_ref),
      join = st_intersects,
      left = FALSE, largest = TRUE
    ) %>% 
      as.data.table() %>%
      .[, geometry := NULL] %>%
      merge(dt_cnty_pci, by = "GEOID", allow.cartesian = TRUE) %>%
      .[, `:=`(
          geog_level = geog_level,
          geog_yr = geog_yr,
          region_geog_level = "county",
          region_geog_yr = 2020 # Consistent with input argument
      )] %>%
      setnames("GEOID", "region_geoid")
      
    dt_regional_pci <- rbind(
      dt_regional_pci, dt_regional_pci_cnty, use.names = TRUE
    )
  }

  # --- 6. Formatting & Return ---
  cols_out <- c("target_gjoin", "target_geoid", "geog_level", "geog_yr", 
                "region_geog_level", "region_geoid", "region_geog_yr", 
                "year", "regional_pci")
                
  dt_out <- dt_regional_pci %>%
    select_by_ref(cols_out) %>%
    setcolorder(cols_out)

  # Check uniqueness: 1 row per target_gjoin per year
  stopifnot(dt_out[, .N, by = .(year, target_gjoin)][, all(N == 1)])

  return(dt_out)
}
