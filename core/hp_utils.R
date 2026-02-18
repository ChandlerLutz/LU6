# core/hp_utils.R

f_get_fmcc_cbsa_hp <- function(file_path_fmcc, dt_cz20, dt_cbsa_shp) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref, pipe_check],
    sf[st_as_sf, st_join, st_transform], nanoparquet[read_parquet]
  )

  dt_cz20 <- copy(dt_cz20) %>%
    as.data.table() %>% 
    .[, geometry := st_transform(geometry, crs = 5070)] %>%
    select_by_ref(c("cz20", "geometry")) %>%
    st_as_sf()
  
  dt_shp <- copy(dt_cbsa_shp) %>%
    pipe_check(.[, unique(year) == 2022]) %>%
    setnames("GEOID", "cbsa") %>%
    setnames("NAME", "cbsa_name") %>%
    select_by_ref(c("cbsa", "cbsa_name", "geometry")) %>%
    .[, geometry := st_transform(geometry, crs = 5070)] %>%
    st_as_sf() %>%
    st_join(dt_cz20, left = TRUE, largest = TRUE) %>%
    as.data.table() %>%
    select_by_ref(c("cbsa", "cz20")) %>%
    pipe_check(.[duplicated(cbsa)] %>% nrow() == 0)

  dt_fmcc_cbsa_hp <- read_parquet(file_path_fmcc) %>% 
    setDT() %>%
    setnames(names(.), janitor::make_clean_names(names(.))) %>%
    .[geo_type == "CBSA"] %>%
    .[, month := sprintf("%02d", as.integer(month))] %>%
    .[, index := as.Date(paste(year, month, "01", sep = "-"))] %>%
    setnames("geo_code", "cbsa") %>%
    setnames("geo_name", "cbsaname") %>%
    setnames("index_nsa", "fmcc_hp_nsa") %>%
    setnames("index_sa", "fmcc_hp_sa") %>%
    select_by_ref(c("cbsa", "cbsaname", "index", "fmcc_hp_nsa", "fmcc_hp_sa")) %>%
    merge(dt_shp, by = "cbsa", all.x = TRUE) %>%
    setcolorder(c("cbsa", "cbsaname", "cz20", "index", "fmcc_hp_nsa", "fmcc_hp_sa")) %>%
    .[order(cbsa, index)] %>% 
    .[, dlog_yoy_fmcc_hp_nsa := log(fmcc_hp_nsa) - shift(log(fmcc_hp_nsa), n = 12),
      by = cbsa] %>%
    .[, dlog_yoy_fmcc_hp_sa := log(fmcc_hp_sa) - shift(log(fmcc_hp_sa), n = 12),
      by = cbsa] %>%
    .[, GEOID := as.character(cbsa)] %>%
    .[, hp_local := fmcc_hp_sa] %>%
    .[, dlog_yoy_hp_local := dlog_yoy_fmcc_hp_sa]
  
  stopifnot(
    all(c("cbsa", "cbsaname", "cz20", "index", "fmcc_hp_nsa", "fmcc_hp_sa",
          "dlog_yoy_fmcc_hp_nsa", "dlog_yoy_fmcc_hp_sa", 
          "GEOID", "hp_local", "dlog_yoy_hp_local") %in% names(dt_fmcc_cbsa_hp)),
    dt_fmcc_cbsa_hp[is.na(cz20)] %>% nrow() == 0
  )

  dt_fmcc_cbsa_hp <- dt_fmcc_cbsa_hp %>%
    setcolorder(c("GEOID", "index"))
  
  return(dt_fmcc_cbsa_hp)
}

f_get_fmcc_natl_hp <- function(file_path_fmcc) {

  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref], nanoparquet[read_parquet]
  )

  dt <- read_parquet(file_path_fmcc) %>% 
    setDT() %>%
    setnames(names(.), janitor::make_clean_names(names(.))) %>%
    .[geo_type == "US"] %>%
    .[, index := as.Date(paste(year, sprintf("%02d", as.integer(month)), "01",
                               sep = "-"))] %>%
    setnames("index_nsa", "fmcc_us_hp_nsa") %>%
    setnames("index_sa", "fmcc_us_hp_sa") %>%
    select_by_ref(c("index", "fmcc_us_hp_nsa", "fmcc_us_hp_sa")) %>%
    setcolorder(c("index", "fmcc_us_hp_nsa", "fmcc_us_hp_sa")) %>%
    .[order(index)] %>%
    .[, dlog_yoy_fmcc_us_hp_nsa := log(fmcc_us_hp_nsa) - shift(log(fmcc_us_hp_nsa),
                                                               n = 12)] %>%
    .[, dlog_yoy_fmcc_us_hp_sa := log(fmcc_us_hp_sa) - shift(log(fmcc_us_hp_sa),
                                                             n = 12)] %>%
    .[, GEOID := "US"] %>%
    .[, hp_local := fmcc_us_hp_sa] %>%
    .[, dlog_yoy_hp_local := dlog_yoy_fmcc_us_hp_sa] %>%
    setcolorder("GEOID", "index")

  return(dt)
}

f_get_fhfa_annual_hpi <- function(geog_level, file_path_raw, dt_to_cz20_cw = NULL) {
  
  box::use(
    data.table[...], magrittr[`%>%`], 
    CLmisc[select_by_ref], readxl[read_xlsx],
    janitor[make_clean_names], stringi[stri_length]
  )

  if (geog_level %notin% c("zip3", "zip5", "cbsa", "county", "tract", "natl"))
    stop("Invalid geog_level")

  if (geog_level == "tract") {
    dt <- fread(file_path_raw, colClasses = "character")
  } else {
    dt <- read_xlsx(file_path_raw, skip = 5) %>% setDT()
  }
  
  setnames(dt, names(dt), make_clean_names(names(dt)))

  if (geog_level == "zip3") {
    dt <- dt %>%
      setnames("three_digit_zip_code", "zip3") %>%
      select_by_ref(c("zip3", "year", "hpi")) %>%
      setnames("hpi", "fhfa_zip3_hpi") %>%
      merge(dt_to_cz20_cw, by.x = "zip3", by.y = "GEOID", all.x = TRUE) %>%
      .[is.na(cz20), cz20 := paste0(zip3, "_")] %>%
      select_by_ref(c("zip3", "cz20", "year", "fhfa_zip3_hpi")) %>%
      setcolorder(c("zip3", "cz20", "year", "fhfa_zip3_hpi")) %>%
      .[order(zip3, year)] %>% 
      .[, dlog_yoy_fhfa_zip3_hpi := log(fhfa_zip3_hpi) - log(shift(fhfa_zip3_hpi, 1)),
        by = zip3] %>%
      .[, GEOID := as.character(zip3)] %>%
      .[, hp_local := fhfa_zip3_hpi] %>%
      .[, dlog_yoy_hp_local := dlog_yoy_fhfa_zip3_hpi]

  } else if (geog_level == "zip5") {
    dt <- dt %>%
      setnames("five_digit_zip_code", "zip5") %>%
      setnames("hpi", "fhfa_zip5_hpi") %>% 
      setcolorder(c("zip5", "year", "fhfa_zip5_hpi")) %>%
      .[year >= 1993] %>% 
      .[order(zip5, year)] %>%
      .[, dlog_yoy_fhfa_zip5_hpi := log(fhfa_zip5_hpi) - log(shift(fhfa_zip5_hpi, 1)),
        by = zip5] %>%
      merge(dt_to_cz20_cw, by.x = "zip5", by.y = "GEOID", all.x = TRUE) %>%
      select_by_ref(c("zip5", "cz20", "year", "fhfa_zip5_hpi",
                      "dlog_yoy_fhfa_zip5_hpi")) %>%
      setcolorder(c("zip5", "cz20", "year", "fhfa_zip5_hpi",
                    "dlog_yoy_fhfa_zip5_hpi")) %>%
      .[, GEOID := as.character(zip5)] %>%
      .[, hp_local := fhfa_zip5_hpi] %>%
      .[, dlog_yoy_hp_local := dlog_yoy_fhfa_zip5_hpi]

    missing_zips <- dt[is.na(cz20), unique(zip5)] %>% sort()
    if (!all(missing_zips %in% c("85144", "85288", "92878"))) {
      warning("Unexpected missing Zip5 CZs found.")
    }

  } else if (geog_level == "cbsa") {
    dt <- dt %>%
      setnames("name", "cbsa_name") %>%
      .[stri_length(cbsa) == 5] %>%
      setnames("hpi", "fhfa_cbsa_hpi") %>%
      select_by_ref(c("cbsa", "cbsa_name", "year", "fhfa_cbsa_hpi")) %>%
      merge(dt_to_cz20_cw, by.x = "cbsa", by.y = "GEOID", all.x = TRUE) %>%
      select_by_ref(c("cbsa", "cbsa_name", "cz20", "year", "fhfa_cbsa_hpi")) %>%
      setcolorder(c("cbsa", "cbsa_name", "cz20", "year", "fhfa_cbsa_hpi")) %>%
      .[order(cbsa, year)] %>% 
      .[, dlog_yoy_fhfa_cbsa_hpi := log(fhfa_cbsa_hpi) - log(shift(fhfa_cbsa_hpi, 1)),
        by = cbsa] %>%
      .[, GEOID := as.character(cbsa)] %>%
      .[, hp_local := fhfa_cbsa_hpi] %>%
      .[, dlog_yoy_hp_local := dlog_yoy_fhfa_cbsa_hpi]

  } else if (geog_level == "county") {
    dt <- dt %>%
      setnames("hpi", "fhfa_cnty_hpi") %>%
      select_by_ref(c("state", "county", "fips_code", "year", "fhfa_cnty_hpi")) %>%
      merge(dt_to_cz20_cw, by.x = "fips_code", by.y = "GEOID", all.x = TRUE) %>%
      select_by_ref(c("state", "county", "fips_code", "cz20", "year",
                      "fhfa_cnty_hpi")) %>%
      setcolorder(c("state", "county", "fips_code", "cz20", "year",
                    "fhfa_cnty_hpi")) %>%
      .[order(fips_code, year)] %>% 
      .[, dlog_yoy_fhfa_cnty_hpi := log(fhfa_cnty_hpi) - log(shift(fhfa_cnty_hpi, 1)),
        by = fips_code] %>%
      .[, GEOID := as.character(fips_code)] %>%
      .[, hp_local := fhfa_cnty_hpi] %>%
      .[, dlog_yoy_hp_local := dlog_yoy_fhfa_cnty_hpi]

  } else if (geog_level == "tract") {
    dt <- dt %>%
      setnames("hpi", "fhfa_trct_hpi") %>%
      .[, fhfa_trct_hpi := as.numeric(fhfa_trct_hpi)] %>%
      .[, year := as.integer(year)] %>%
      .[year >=  1993] %>%
      .[order(state_abbr, tract, year)] %>%
      select_by_ref(c("state_abbr", "tract", "year", "fhfa_trct_hpi")) %>%
      merge(dt_to_cz20_cw, by.x = "tract", by.y = "GEOID", all.x = TRUE) %>%
      setcolorder(c("state_abbr", "tract", "cz20", "year", "fhfa_trct_hpi")) %>%
      .[, dlog_yoy_fhfa_trct_hpi := log(fhfa_trct_hpi) - log(shift(fhfa_trct_hpi, 1)),
        by = .(state_abbr, tract)] %>%
      .[, GEOID := as.character(tract)] %>%
      .[, hp_local := fhfa_trct_hpi] %>%
      .[, dlog_yoy_hp_local := dlog_yoy_fhfa_trct_hpi]
      
  } else if (geog_level == "natl") {
    dt <- dt %>%
      setnames("hpi", "fhfa_natl_hpi") %>%
      select_by_ref(c("year", "fhfa_natl_hpi")) %>%
      setcolorder(c("year", "fhfa_natl_hpi")) %>%
      .[order(year)] %>% 
      .[, dlog_yoy_fhfa_natl_hpi := log(fhfa_natl_hpi) - log(shift(fhfa_natl_hpi, 1))] %>%
      .[, GEOID := "US"] %>%
      .[, hp_local := fhfa_natl_hpi] %>%
      .[, dlog_yoy_hp_local := dlog_yoy_fhfa_natl_hpi]
  }

  dt[, index := as.Date(paste0(year, "-01-01"))]
  dt <- dt %>%
    .[year >= "1980"] %>%
    setcolorder(c("GEOID", "index"))

  return(dt)
}

f_get_fhfa_qtrly_at_hpi <- function(geog_level, file_path_raw, dt_to_cz20_cw = NULL) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref], 
    readxl[read_excel], janitor[make_clean_names]
  )

  if (geog_level %notin% c("zip3", "natl")) {
    stop("geog_level must be one of 'zip3', 'natl'")
  }

  if (geog_level == "zip3") {
    
    dt <- read_excel(file_path_raw, skip = 4) %>% 
      setDT() %>%
      setnames(names(.), make_clean_names(names(.))) %>%
      .[index_type == "Native 3-Digit ZIP index"] %>%
      setnames("three_digit_zip_code", "zip3") %>%
      .[, zip3 := sprintf("%03d", as.integer(zip3))] %>%
      .[, index := as.Date(paste0(year, "-",
                                  sprintf("%02d", (as.integer(quarter) - 1) * 3 + 1), "-01"))] %>%
      setnames("index_nsa", "hpi") %>%
      select_by_ref(c("zip3", "index", "hpi")) %>%
      merge(dt_to_cz20_cw, by.x = "zip3", by.y = "GEOID", all.x = TRUE) %>%
      .[is.na(cz20), cz20 := paste0(zip3, "_")] %>%
      setcolorder(c("zip3", "index", "cz20", "hpi")) %>%
      .[order(zip3, index)] %>%
      .[, dlog_yoy_hpi := log(hpi) - shift(log(hpi), n = 4), by = zip3] %>%
      .[, GEOID := as.character(zip3)] %>%
      .[, hp_local := hpi] %>%
      .[, dlog_yoy_hp_local := dlog_yoy_hpi]

  } else if (geog_level == "natl") {

    dt <- fread(file_path_raw, colClasses = "character") %>%
      setnames(names(.), c("region", "yr", "qtr", "hpi")) %>%
      .[region == "USA"] %>%
      .[, index := as.Date(paste0(yr, "-", sprintf("%02d", (as.numeric(qtr) - 1) * 3 + 1), "-01"))] %>%
      setnames("hpi", "fhfa_natl_hpi") %>%
      .[, fhfa_natl_hpi := as.numeric(fhfa_natl_hpi)] %>%
      select_by_ref(c("index", "fhfa_natl_hpi")) %>%
      setcolorder(c("index", "fhfa_natl_hpi")) %>%
      .[order(index)] %>%
      .[, dlog_yoy_fhfa_natl_hpi := log(fhfa_natl_hpi) - shift(log(fhfa_natl_hpi),
                                                               n = 4)] %>%
      .[, GEOID := "US"] %>%
      .[, hp_local := fhfa_natl_hpi] %>%
      .[, dlog_yoy_hp_local := dlog_yoy_fhfa_natl_hpi]
  }

  dt <- setcolorder(dt, c("GEOID", "index"))

  return(dt)
}

f_get_zillow_hp_natl <- function(file_path_metro) {

  box::use(
    data.table[...], magrittr[`%>%`], nanoparquet[read_parquet],
    lubridate[floor_date], stringi[stri_extract, stri_length], CLmisc[select_by_ref]
  )

  dt <- read_parquet(file_path_metro) %>% setDT() %>%
    .[RegionType == "country"] %>%
    melt(id.vars = c("RegionID", "SizeRank", "RegionName", "RegionType", "StateName"),
         measure.vars = patterns("^20"), variable.name = "index", value.name = "zhvi",
         variable.factor = FALSE) %>%
    select_by_ref(c("index", "zhvi")) %>%
    .[, index := floor_date(as.Date(index), unit = "month")] %>%
    setorder(index) %>%
    .[, hp_natl := zhvi] %>%
    .[, dlog_yoy_hp_natl := log(hp_natl) - shift(log(hp_natl), n = 12)]

  return(dt)

}


f_get_zillow_hp_cbsa <- function(file_path_metro, file_path_county, cbsa_shp, cz20_shp) {
  
  box::use(
    data.table[...], magrittr[`%>%`], nanoparquet[read_parquet],
    lubridate[floor_date], stringi[stri_extract, stri_length], CLmisc[select_by_ref]
  )

  dt_metro_full_names <- read_parquet(file_path_county) %>% 
    setDT() %>%
    select_by_ref(c("Metro", "State")) %>%
    .[Metro != ""] %>%
    .[!duplicated(Metro)] %>%
    .[, metro_first_name := stri_extract(Metro, regex = "^([^-,]+)")] %>%
    .[, RegionName := paste0(metro_first_name, ", ", State)] %>%
    .[, RegionType := "msa"] %>%
    setnames("State", "StateName")

  dt_raw <- read_parquet(file_path_metro) %>% setDT()
  
  dt_metro_names <- dt_raw %>%
    .[!duplicated(RegionID), .(RegionID, RegionName, RegionType, StateName)] %>%
    .[stri_length(RegionName) > 0] %>%
    .[!duplicated(RegionName)] %>%
    merge(dt_metro_full_names, by = c("RegionType", "StateName", "RegionName"),
          all.x = TRUE) 

  dt_cbsa_cz20 <- f_cw_geo_to_cz20(cbsa_shp, cz20_shp, id_col = "GEOID") %>%
    setnames("GEOID", "cbsa")

  dt_cbsa_ids <- cbsa_shp %>%
    as.data.table() %>%
    select_by_ref(c("GEOID", "NAME")) %>%
    setnames("GEOID", "cbsa") %>%
    setnames("NAME", "cbsaname")

  dt_crosswalk <- merge(dt_metro_names[, .(RegionID, cbsaname = Metro)], dt_cbsa_ids,
                        by = "cbsaname", all.x = TRUE) %>%
    merge(dt_cbsa_cz20, by = "cbsa", all.x = TRUE)

  dt_out <- dt_raw %>% 
    melt(measure.vars = patterns("^20"), variable.name = "index", value.name = "zhvi",
         variable.factor = FALSE) %>%
    .[, index := floor_date(as.Date(index), unit = "month")] %>%
    merge(dt_crosswalk, by = "RegionID", all.x = TRUE) %>%
    .[, cbsa_yr := 2022] %>% 
    select_by_ref(c("RegionID", "cbsa", "cbsaname", "cbsa_yr", "cz20", "SizeRank", 
                    "RegionName", "RegionType", "StateName", "index", "zhvi")) %>%
    setcolorder(c("RegionID", "cbsa", "cbsaname", "cbsa_yr", "cz20", "SizeRank",
                  "RegionName", "RegionType", "StateName", "index", "zhvi")) %>%
    .[, GEOID := as.character(cbsa)] %>%
    .[, hp_local := zhvi] %>%
    .[order(GEOID, index)] %>%
    .[, dlog_yoy_hp_local := log(hp_local) - shift(log(hp_local), n = 12),
      by = GEOID] %>%
    setcolorder(c("GEOID", "index"))
    
  return(dt_out)
}

f_get_zillow_hp_county <- function(file_path_raw, dt_to_cz20_cw) {
  
  box::use(
    data.table[...], magrittr[`%>%`], nanoparquet[read_parquet],
    lubridate[floor_date], CLmisc[select_by_ref]
  )
  
  dt <- read_parquet(file_path_raw) %>% 
    setDT() %>%
    melt(measure.vars = patterns("^20"), variable.name = "index", value.name = "zhvi",
         variable.factor = FALSE) %>%
    .[, index := floor_date(as.Date(index), unit = "month")] %>%
    .[, cntyfp := paste0(StateCodeFIPS, MunicipalCodeFIPS)] %>%
    merge(dt_to_cz20_cw, by.x = "cntyfp", by.y = "GEOID", all.x = TRUE) %>%
    setcolorder(c("cntyfp", "RegionID", "cz20", "index", "zhvi")) %>%
    .[, GEOID := as.character(cntyfp)] %>%
    .[, hp_local := zhvi] %>%
    .[order(GEOID, index)] %>%
    .[, dlog_yoy_hp_local := log(hp_local) - shift(log(hp_local), n = 12),
      by = GEOID] %>%
    setcolorder(c("GEOID", "index"))

  return(dt)
}

f_get_zillow_hp_zip <- function(file_path_raw, dt_to_cz20_cw) {
  
  box::use(
    data.table[...], magrittr[`%>%`], nanoparquet[read_parquet],
    lubridate[floor_date], CLmisc[select_by_ref]
  )
  
  dt <- read_parquet(file_path_raw) %>% 
    setDT() %>%
    melt(measure.vars = patterns("^20"), variable.name = "index", value.name = "zhvi",
         variable.factor = FALSE) %>%
    .[, index := floor_date(as.Date(index), unit = "month")] %>%
    .[, zip := sprintf("%05d", as.integer(RegionName))] %>%
    merge(dt_to_cz20_cw, by.x = "zip", by.y = "GEOID", all.x = TRUE) %>%
    setcolorder(c("zip", "RegionID", "cz20", "index", "zhvi")) %>%
    .[, GEOID := as.character(zip)] %>%
    .[, hp_local := zhvi] %>%
    .[order(GEOID, index)] %>%
    .[, dlog_yoy_hp_local := log(hp_local) - shift(log(hp_local), n = 12),
      by = GEOID] %>%
    setcolorder(c("GEOID", "index"))

  return(dt)
}
