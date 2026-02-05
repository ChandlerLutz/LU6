# core/bea_utils.R

f_get_bea_cbsa_per_capita_income <- function(file_msa, file_micro, file_csa, file_div) {
  
  box::use(data.table[...], magrittr[`%>%`])

  dt_msa <- fread(file_msa, skip = 3, nrows = 385, colClasses = "character")
  dt_micropolitan <- fread(file_micro, skip = 3, nrows = 543, colClasses = "character")
  dt_csa <- fread(file_csa, skip = 3, nrows = 172, colClasses = "character")
  dt_metro_division <- fread(file_div, skip = 3, nrows = 31, colClasses = "character")

  dt_cbsa <- rbind(dt_msa, dt_micropolitan, dt_csa, dt_metro_division)

  stopifnot(
    dt_cbsa[duplicated(GeoFips)] %>% nrow() == 0,
    dt_cbsa[duplicated(GeoName)] %>% nrow() == 0
  )

  dt_cbsa <- dt_cbsa %>%
    melt(id.vars = c("GeoFips", "GeoName"), variable.name = "year",
         value.name = "per_capita_income", 
         variable.factor = FALSE) %>%
    .[!(GeoFips == "00998" & grepl("United State", GeoName))] %>%
    setnames("GeoFips", "GEOID") %>%
    .[, index := as.Date(paste0(year, "-12-01"))] %>%
    setcolorder(c("GEOID", "GeoName", "year", "index", "per_capita_income")) %>%
    .[, per_capita_income := as.numeric(per_capita_income)] %>%
    .[order(GEOID, index)] %>%
    .[, dlog_per_capita_income := log(per_capita_income) - shift(log(per_capita_income),
                                                                 1),
      by = .(GEOID)] %>%
    .[, cbsa_geog_yr := 2020] %>%
    setcolorder(c("GEOID", "GeoName", "cbsa_geog_yr", "year", "index",
                  "per_capita_income","dlog_per_capita_income"))
  
  return(dt_cbsa)
}

# Reads files, melts, and types. Returns the raw combined table.
f_load_raw_bea_cainc1 <- function(file_paths) {
  
  box::use(data.table[...], magrittr[`%>%`])

  f_read_one <- function(fp) {
    # Detect notes and read
    lines <- readLines(fp)
    # Using 'head' to safely find the start even in empty files
    idx <- grep("Note: See the included footnote", lines)[1] 
    n_read <- if (is.na(idx)) Inf else idx - 1
    
    fread(fp, nrows = n_read, colClasses = "character")
  }

  # Read and Combine
  dt_raw <- lapply(file_paths, f_read_one) %>%
    rbindlist(use.names = TRUE, fill = TRUE) %>%
    setnames("GeoFIPS", "cntyfp") %>%
    setnames("GeoName", "cntyname") %>%
    .[, c("IndustryClassification", "Region") := NULL] %>%
    melt(id.vars = c("cntyfp", "cntyname", "TableName", "LineCode", "Description",
                     "Unit"),
         variable.name = "year", value.name = "value", variable.factor = FALSE) %>%
    .[, variable := fcase(
        grepl("Personal income.*thousands of dollars", Description), "total_income_thousands",
        grepl("Population.*persons", Description), "population",
        grepl("Per capita personal income.*dollars", Description), "per_capita_income_dollars"
    )] %>%
    .[!is.na(variable)] # Drop rows that didn't match our fcase

  # Pivot (dcast)
  dt_wide <- dt_raw %>%
    dcast(cntyfp + cntyname + year ~ variable, value.var = "value") %>%
    .[, `:=`(
        total_income_thousands = as.numeric(total_income_thousands),
        population = as.numeric(population),
        per_capita_income_dollars = as.numeric(per_capita_income_dollars),
        year = as.integer(year)
    )] %>%
    .[!is.na(total_income_thousands) | !is.na(population) | !is.na(per_capita_income_dollars)]

  return(dt_wide)
}

f_process_bea_state_income <- function(dt_raw) {
  
  box::use(data.table[...], magrittr[`%>%`])
  
  dt_states <- dt_raw[grepl("000$", cntyfp)] %>%
    .[, stfp := substr(cntyfp, 1, 2)] %>%
    .[, cntyfp := NULL] %>%
    setnames("cntyname", "stname") %>%
    setcolorder(c("stfp", "stname", "year", "total_income_thousands",
                  "population", "per_capita_income_dollars"))
  
  return(dt_states)
}

f_process_bea_county_income <- function(dt_raw, cnty_shp) {
  
  box::use(
    data.table[...], magrittr[`%>%`], 
    CLmisc[select_by_ref]
  )

  # Filter for counties
  dt_counties <- dt_raw[!grepl("000$", cntyfp)]

  # Prepare Shapefile Metadata
  dt_shp_ref <- copy(cnty_shp) %>%
    select_by_ref(c("GEOID", "NAME", "year")) %>%
    setnames("NAME", "shp_cnty_nm") %>% 
    setnames("year", "census_yr") %>%
    .[substr(GEOID, 1, 2) %notin% c("02", "15", "72")] 

  # Merge
  dt_final <- merge(
    dt_counties, dt_shp_ref,
    by.x = "cntyfp", by.y = "GEOID",
    all = TRUE
  ) %>%
    .[, is_2020_cnty := fifelse(census_yr == 2020 & !is.na(census_yr), 1L, 0L)] %>%
    setcolorder(c("cntyfp", "cntyname", "shp_cnty_nm", "census_yr", "is_2020_cnty",
                  "year", "total_income_thousands",
                  "population", "per_capita_income_dollars")) %>%
    .[order(cntyfp, year)]

  return(dt_final)
}

f_get_regional_bea_income <- function(target_shp, cbsa_shp_2022, cnty_shp_2020,  
                                      dt_bea_cbsa_pci, dt_bea_cnty_pci,
                                      geog_level, geog_yr) {
  
  box::use(
    data.table[...], magrittr[`%>%`], 
    sf[st_transform, st_join, st_intersects, st_as_sf], 
    CLmisc[select_by_ref, pipe_check]
  )

  dt_target_shp <- copy(target_shp) %>% 
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "GEOID", "geometry")) %>%
    setcolorder(c("GISJOIN", "GEOID", "geometry")) %>%
    setnames("GISJOIN", "target_gjoin") %>%
    setnames("GEOID", "target_geoid") %>% 
    .[, geometry := st_transform(geometry, crs = 5070)]

  stopifnot(names(dt_target_shp) == c("target_gjoin", "target_geoid", "geometry"))

  # --- 2. Prepare Reference Shapes ---
  # We rename them internally to 'ref' for clarity, but the input
  # ensures we are using the correct vintage.
  
  dt_cbsa_ref <- copy(cbsa_shp_2022) %>%
    select_by_ref(c("GEOID", "geometry")) %>%
    setcolorder(c("GEOID", "geometry")) %>%
    .[, geometry := st_transform(geometry, crs = 5070)]

  dt_cnty_ref <- copy(cnty_shp_2020) %>%
    select_by_ref(c("GEOID", "geometry")) %>%
    setcolorder(c("GEOID", "geometry")) %>%
    .[, geometry := st_transform(geometry, crs = 5070)]
    
  # --- 3. Prepare Reference Income Data ---
  dt_cbsa_pci <- copy(dt_bea_cbsa_pci) %>%
    .[GEOID %in% dt_cbsa_ref$GEOID] %>%
    select_by_ref(c("GEOID", "year", "per_capita_income")) %>%
    setcolorder(c("GEOID", "year", "per_capita_income"))

  dt_cnty_pci <- copy(dt_bea_cnty_pci) %>%
    select_by_ref(c("cntyfp", "year", "per_capita_income_dollars")) %>%
    setnames("cntyfp", "GEOID") %>% 
    setnames("per_capita_income_dollars", "per_capita_income") %>%
    setcolorder(c("GEOID", "year", "per_capita_income"))

  # --- 4. Spatial Join: Target -> CBSA (Primary) ---
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
        region_geog_yr = 2022  
    )] %>%
    setnames("GEOID", "region_geoid") %>%
    setnames("per_capita_income", "regional_pci")

  # --- 5. Spatial Join: Leftovers -> County (Fallback) ---
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
      setnames("GEOID", "region_geoid") %>%
      setnames("per_capita_income", "regional_pci")
      
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

  stopifnot(dt_out[, .N, by = .(year, target_gjoin)][, all(N == 1)])

  return(dt_out)
}
