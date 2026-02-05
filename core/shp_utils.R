## core/shp_utils.R

f_get_nhgis_shp_metadata <- function() {
  
  box::use(
    keyring[key_get], magrittr[`%>%`], data.table[setDT],
    ipumsr[set_ipums_api_key, get_metadata_catalog]
  )

  ipums_key <- key_get("ipums_api_key")
  set_ipums_api_key(ipums_key, save = FALSE)
  
  get_metadata_catalog(
    collection = "nhgis", metadata_type = "shapefiles"
  ) %>%
    setDT() %>% return()
}

f_get_cnty_shp <- function(year, dt_nhgis_shp_metadata) {

  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref, get_rnhgis_shp] 
  )

  yr_tmp <- year
  
  nhgis_shp_nm <- dt_nhgis_shp_metadata %>%
    .[geographic_level == "County" & year == c(yr_tmp)] %>%
    .[order(extent)] %>%
    .[.N, name]

  if (length(nhgis_shp_nm) == 0)
    stop(sprintf("Error: In `f_get_cnty_shp()` shapefile not found for year %s", year))

  dt_cnty_shp <- get_rnhgis_shp(shp = nhgis_shp_nm) %>%
    as.data.table() %>%
    .[, GEOID := paste0(substr(GISJOIN, 2, 3), substr(GISJOIN, 5, 7))]

  name_var <- names(dt_cnty_shp) %>% .[grepl("^NAME|^NHGISNAM", .)] %>% .[1]
  dt_cnty_shp <- setnames(dt_cnty_shp, name_var, "NAME")

  dt_cnty_shp <- dt_cnty_shp %>%
    .[, year := c(yr_tmp)] %>%
    select_by_ref(c("GISJOIN", "GEOID", "NAME", "year", "geometry")) %>%
    setcolorder(c("GISJOIN", "GEOID", "NAME", "year", "geometry"))

  return(dt_cnty_shp)
}

# core/shp_utils.R

f_get_msa_cbsa_shp <- function(year, dt_nhgis_shp_metadata, file_path_1999 = NULL,
                               file_path_2007 = NULL) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref, get_rnhgis_shp],
    sf[st_set_crs]
  )
  
  yr_tmp <- year
  
  if (year %in% c(1990, 2000) || year >= 2009) {
    
    nhgis_shp_nm <- dt_nhgis_shp_metadata %>%
      .[year == c(yr_tmp) & grepl("^Metropolitan Statistical Area",
                                  geographic_level)] %>%
      .[order(extent)] %>%
      .[.N, name]
    
    if (length(nhgis_shp_nm) == 0)
      stop(sprintf("Error: In `f_get_msa_cbsa_shp()` shapefile not found for year %s",
                   year))
    
    dt_msa_cbsa_shp <- get_rnhgis_shp(shp = nhgis_shp_nm) %>%
      as.data.table()

    if ("NAME" %notin% names(dt_msa_cbsa_shp)) {
      name_var <- names(dt_msa_cbsa_shp) %>% .[grepl("^NAME", .)]
      if (length(name_var) == 0) {
        dt_msa_cbsa_shp <- dt_msa_cbsa_shp[, NAME := "Name Unknown"]
      } else {
        name_var <- name_var[1]
        setnames(dt_msa_cbsa_shp, name_var, "NAME")
      }
    }

    cbsa_code_nm <- names(dt_msa_cbsa_shp) %>% .[grepl("^CBSAFP|^MSACMSA$", .)]
    if (length(cbsa_code_nm) > 1)
      stop("Error: more than one msa/cbsa code column found")

    dt_msa_cbsa_shp <- dt_msa_cbsa_shp %>%
      select_by_ref(c(cbsa_code_nm, "NAME", "geometry")) %>%
      .[, year := yr_tmp] %>%
      setnames(cbsa_code_nm, "GEOID")

    if ("GISJOIN" %notin% names(dt_msa_cbsa_shp))
      dt_msa_cbsa_shp[, GISJOIN := paste0("G", GEOID)]

    dt_msa_cbsa_shp <- dt_msa_cbsa_shp %>%
      setcolorder(c("GISJOIN", "GEOID", "NAME", "year", "geometry"))      
    
  } else if (year == 1999) {
    
    if (is.null(file_path_1999)) stop("Must provide file_path_1999 for year 1999")
    
    dt_msa_cbsa_shp <- readRDS(file_path_1999) %>%
      st_set_crs(4326) %>%
      as.data.table() %>%
      .[, GEOID := sprintf("%04.f", GEOID)] %>%
      .[, year := yr_tmp] %>%
      .[, GISJOIN := paste0("G", GEOID)] %>%
      select_by_ref(c("GISJOIN", "GEOID", "NAME", "year", "geometry")) %>%
      setcolorder(c("GISJOIN", "GEOID", "NAME", "year", "geometry"))
    
  } else if (year == 2007) {
    
    if (is.null(file_path_2007)) stop("Must provide file_path_2007 for year 2007")
    
    dt_msa_cbsa_shp <- readRDS(file_path_2007) %>%
      .[, year := yr_tmp] %>%
      setnames("CBSAFP", "GEOID") %>%
      .[, GISJOIN := paste0("G", GEOID)] %>%
      select_by_ref(c("GISJOIN", "GEOID", "NAME", "year", "geometry")) %>%
      setcolorder(c("GISJOIN", "GEOID", "NAME", "year", "geometry"))
      
  } else {
    stop("Error: MSA/CBSA shapefile not found for year %s", year)
  }

  stopifnot(names(dt_msa_cbsa_shp) == c("GISJOIN", "GEOID", "NAME", "year", "geometry"))

  return(dt_msa_cbsa_shp)
}

f_get_cz20_shp <- function(file_path_cnty23_cz20_cw, dt_cnty_shp) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref, pipe_check],
    dplyr[summarize, group_by, ungroup],
    sf[st_as_sf, st_is_empty, st_transform, st_union, st_cast]
  )

  dt_cnty_shp <- copy(dt_cnty_shp) %>%
    pipe_check(.[, all(year == 2023)]) %>%
    select_by_ref(c("GEOID", "geometry")) %>%
    setnames("GEOID", "cntyfp20") %>%
    .[, geometry := st_transform(geometry, 5070)]

  dt_cz20_shp <- fread(file_path_cnty23_cz20_cw, colClasses = "character") %>%
    setnames("PreliminaryCZ2020", "cz20") %>%
    .[, cz20 := sprintf("%03s", cz20)] %>%
    setnames("FIPStxt", "cntyfp20") %>%
    .[, cntyfp20 := sprintf("%05s", cntyfp20)] %>%
    select_by_ref(c("cz20", "cntyfp20")) %>%
    merge(dt_cnty_shp, by = "cntyfp20", all = TRUE) %>%
    pipe_check(nrow(.[st_is_empty(geometry)]) == 0) %>% 
    st_as_sf() %>%
    group_by(cz20) %>%
    summarize(geometry = st_union(geometry)) %>%
    ungroup() %>%
    as.data.table() %>%
    .[, geometry := st_cast(geometry, to = "MULTIPOLYGON")]

  stopifnot(
    dt_cz20_shp[duplicated(cz20)] %>% nrow() == 0,
    dt_cz20_shp[st_is_empty(geometry)] %>% nrow() == 0
  )

  return(dt_cz20_shp)
}
  
f_cw_geo_to_cz20 <- function(dt_geo_shp, dt_cz20_shp, id_col) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    sf[st_transform, st_join, st_as_sf]
  )

  dt_geo <- dt_geo_shp %>%
    as.data.table() %>% copy()

  if (id_col %notin% names(dt_geo)) {
    stop(sprintf("Column '%s' not found in shapefile.", id_col))
  }

  setnames(dt_geo, id_col, "GEOID")
  
  dt_geo <- dt_geo %>%
    select_by_ref(c("GEOID", "geometry")) %>% 
    .[, geometry := st_transform(geometry, crs = 5070)] %>%
    st_as_sf()

  dt_cz <- dt_cz20_shp %>%
    as.data.table() %>% copy() %>%
    select_by_ref(c("cz20", "geometry")) %>%
    .[, geometry := st_transform(geometry, crs = 5070)] %>%
    st_as_sf()

  dt_out <- st_join(dt_geo, dt_cz, largest = TRUE, left = TRUE) %>%
    as.data.table() %>%
    select_by_ref(c("GEOID", "cz20"))
  
  if (nrow(dt_out[duplicated(GEOID)]) > 0) {
    stop("Duplicate IDs found in GEOID after join")
  }
  
  return(dt_out)
}

f_get_tract_to_nhgis_tract_cw <- function(dt_nhgis_cw_raw, 
                                          shp_trct_1980,
                                          shp_trct_1990,
                                          shp_trct_2000,
                                          shp_trct_2010,
                                          shp_trct_2020) {
  
  box::use(
    data.table[...], magrittr[`%>%`], 
    CLmisc[select_by_ref], stringi[stri_length]
  )

  dt_base <- copy(dt_nhgis_cw_raw)
    
  colorder_orig <- names(dt_base)

  # 2. Process 1980
  dt_1980 <- shp_trct_1980 %>%
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "TRACT")) %>%
    setnames("GISJOIN", "GJOIN1980") %>%
    setnames("TRACT", "TRACT1980") %>%
    .[, TRACT1980 := sprintf("%06d", TRACT1980)]
  
  stopifnot(dt_1980[stri_length(TRACT1980) != 6, .N] == 0)

  # 3. Process 1990
  dt_1990 <- shp_trct_1990 %>%
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "TRACT")) %>%
    setnames("GISJOIN", "GJOIN1990") %>%
    setnames("TRACT", "TRACT1990") %>%
    .[, TRACT1990 := sprintf("%06d", TRACT1990)]
    
  stopifnot(dt_1990[stri_length(TRACT1990) != 6, .N] == 0)

  # 4. Process 2000
  dt_2000 <- shp_trct_2000 %>%
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "TRACTCE00")) %>%
    setnames("GISJOIN", "GJOIN2000") %>%
    setnames("TRACTCE00", "TRACT2000")

  stopifnot(dt_2000[stri_length(TRACT2000) != 6, .N] == 0)

  # 5. Process 2010
  dt_2010 <- shp_trct_2010 %>%
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "TRACTCE10")) %>%
    setnames("GISJOIN", "GJOIN2010") %>%
    setnames("TRACTCE10", "TRACT2010")

  stopifnot(dt_2010[stri_length(TRACT2010) != 6, .N] == 0)

  # 6. Process 2020
  dt_2020 <- shp_trct_2020 %>%
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "TRACTCE")) %>%
    setnames("GISJOIN", "GJOIN2020") %>%
    setnames("TRACTCE", "TRACT2020")

  stopifnot(dt_2020[stri_length(TRACT2020) != 6, .N] == 0)

  # 7. Merge All
  dt_out <- dt_base %>%
    merge(dt_1980, by = "GJOIN1980", all.x = TRUE) %>%
    merge(dt_1990, by = "GJOIN1990", all.x = TRUE) %>%
    merge(dt_2000, by = "GJOIN2000", all.x = TRUE) %>%
    merge(dt_2010, by = "GJOIN2010", all.x = TRUE) %>%
    merge(dt_2020, by = "GJOIN2020", all.x = TRUE) %>%
    setcolorder(colorder_orig)

  return(dt_out)
}
