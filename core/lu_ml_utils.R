## core/lu_ml_utils.R

f_run_lu_ml <- function(dt_est, dt_lu, run_in_parallel = FALSE) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    lu.ml[lu_ml_xgboost_time_varying]
  )
  
  required_cols <- c("GEOID", "index", "hp.target")
  
  if (nrow(dt_est) == 0) stop("dt_est has zero rows.")
  if (nrow(dt_lu) == 0) stop("dt_lu has zero rows.")
  
  if (!all(required_cols %in% names(dt_est))) {
    missing <- setdiff(required_cols, names(dt_est))
    stop(sprintf("dt_est is missing columns: %s", paste(missing, collapse = ", ")))
  }
  
  dt_est <- copy(dt_est) %>%
    select_by_ref(required_cols) %>%
    setcolorder(required_cols)
  
  unique_indices <- unique(dt_est$index)
  n_indices <- length(unique_indices)
  
  # Helper to check duplicates
  check_dups <- function(dt) {
    if (anyDuplicated(dt$GEOID) > 0) {
      stop(sprintf("Duplicate GEOIDs found for index: %s", unique(dt$index)[1]))
    }
  }
  
  if (run_in_parallel == FALSE) {
    
    dt_list <- split(dt_est, by = "index")
    
    f_wrkr <- function(dt_sub) {
      check_dups(dt_sub) # Better error msg
      
      lu_ml_xgboost_time_varying(
        DT.hp = dt_sub,
        DT.lu = dt_lu,
        run_in_parallel = FALSE
      )
    }
    
    dt_res <- lapply(dt_list, f_wrkr) %>%
      rbindlist(use.names = TRUE)
      
  } else if (run_in_parallel == TRUE && n_indices == 1) {
    
    check_dups(dt_est)
    
    dt_res <- lu_ml_xgboost_time_varying(
      DT.hp = dt_est, 
      DT.lu = dt_lu, 
      run_in_parallel = TRUE
    )
    
  } else if (run_in_parallel == TRUE && n_indices > 1) {
    
    dt_list <- split(dt_est, by = "index")
    
    f_wrkr <- function(dt_sub) {
      check_dups(dt_sub)
      
      lu_ml_xgboost_time_varying(
        DT.hp = dt_sub, 
        DT.lu = dt_lu,
        run_in_parallel = FALSE
      )
    }
    
    options(future.globals.maxSize = 10e03 * 1024^2) 
    n_workers <- max(1, future::availableCores() - 4)
    future::plan(future::multisession, workers = n_workers)
    
    dt_res <- future.apply::future_lapply(
      dt_list, f_wrkr, future.seed = TRUE
    ) %>%
      rbindlist(use.names = TRUE)
    
    future::plan(future::sequential)
  }
  
  return(dt_res)
}


f_get_fmcc_cbsa_hpi_lu_ml_panel <- function(dt_fmcc_hp_panel, file_path_lu,
                                            dt_regional_pci_panel, dt_hm_cycles,
                                            dt_cbsa_shp) {

  box::use(
    data.table[...], magrittr[`%>%`], lfe[felm],
    nanoparquet[read_parquet], CLmisc[select_by_ref]
  )

  dt_pci_panel <- copy(dt_regional_pci_panel) %>%
    setnames("target_geoid", "cbsa") %>%
    select_by_ref(c("cbsa", "year", "regional_pci")) %>%
    .[, year := as.integer(year)] %>%
    .[order(cbsa, year)] %>%
    .[, dlog_yoy_regional_pci := log(regional_pci) - shift(log(regional_pci)),
      by = .(cbsa)]
    

  dt_hp <- copy(dt_fmcc_hp_panel) %>%
    select_by_ref(c("cbsa", "cbsaname", "cz20", "index", "fmcc_hp_sa",
                    "dlog_yoy_fmcc_hp_sa")) %>%
    .[, year := as.integer(year(index))] %>%
    .[!is.na(dlog_yoy_fmcc_hp_sa)] %>%
    merge(dt_pci_panel, by = c("cbsa", "year")) %>%
    .[, index_char := as.character(index)]

  dt_hp <- dt_hp[dt_hm_cycles,
                 on = .(index >= cycle_start_date, index <= cycle_end_date), 
                 hm_cycle := i.cycle_label] %>%
    .[!is.na(hm_cycle)]

  dt_hp[, dlog_yoy_fmcc_hp_sa_resid := felm(
      dlog_yoy_fmcc_hp_sa ~ dlog_yoy_regional_pci:hm_cycle | index_char
    )$residuals]

  dt_val <- dt_hp[!duplicated(cbsa), .(cbsa, cbsaname)] %>%
    merge(dt_cbsa_shp, by.x = "cbsa", by.y = "GEOID", all.x = TRUE)
  
  if (nrow(dt_val[is.na(geometry)]) > 0)
    stop("Validation Failed: Some CBSAs in the HP data are missing from the 2022 Shapefile.")

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("GEOID", grep("unavailable", names(.), value = TRUE))) %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))]

  dt_input <- dt_hp %>%
    .[, .(GEOID = cbsa, index, hp.target = dlog_yoy_fmcc_hp_sa_resid)]

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "cbsa")

  dt_out <- merge(dt_hp, dt_lu_ml, by = c("cbsa", "index"), all.x = TRUE) %>%
    select_by_ref(c("cbsa", "cbsaname", "cz20", "index", "hm_cycle", "fmcc_hp_sa",
                    "dlog_yoy_fmcc_hp_sa", "regional_pci", "dlog_yoy_regional_pci",
                    "dlog_yoy_fmcc_hp_sa_resid", "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("cbsa", "cbsaname", "cz20", "index", "hm_cycle", "fmcc_hp_sa",
                    "dlog_yoy_fmcc_hp_sa", "regional_pci", "dlog_yoy_regional_pci",
                    "dlog_yoy_fmcc_hp_sa_resid", "hp.target", "lu_ml_xgboost"))

  return(dt_out)
}

f_get_fhfa_cbsa_annual_lu_ml_panel <- function(dt_hp, file_path_lu,
                                               dt_regional_pci_panel, 
                                               dt_hm_cycles, dt_shp) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet], sf[st_is_empty], lfe[felm]
  )

  if (is.null(dt_shp)) stop("dt_shp is required for CBSA level")

  dt_hp <- copy(dt_hp) %>% 
    .[, year := as.integer(year)] %>% 
    .[year >= 1980]

  dt_hp_cbsas <- dt_hp[!duplicated(cbsa), .(cbsa, cbsa_name)]

  dt_val <- merge(dt_hp_cbsas, dt_shp, by.x = "cbsa", by.y = "GEOID", all.x = TRUE)
  if (nrow(dt_val[is.na(geometry) | st_is_empty(geometry)]) > 0) 
    stop("Error: CBSA shapefile mismatch with FHFA data.")

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("GEOID", grep("unavailable", names(.), value = TRUE))) %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))]

  dt_val_lu <- merge(dt_hp_cbsas[!grepl("AK$|HI$", cbsa_name)], dt_lu,
                     by.x = "cbsa", by.y = "GEOID", all.x = TRUE)
  if (nrow(dt_val_lu[is.na(slope_unavailable_cbsa_000_pct_buffer)]) > 0) 
    stop("Error: LU data mismatch with FHFA data.")

  dt_pci <- copy(dt_regional_pci_panel) %>%
    .[, year := as.integer(year)] %>%
    setorder(target_geoid, year) %>%
    .[, dlog_yoy_regional_pci := log(regional_pci) - shift(log(regional_pci)),
      by = target_geoid] %>%
    .[year >= 1980]

  dt_est_data <- merge(
    dt_hp, dt_pci[, .(cbsa = target_geoid, year, dlog_yoy_regional_pci)],
    by = c("cbsa", "year"), all.x = TRUE
  ) %>%
    .[!is.na(dlog_yoy_fhfa_cbsa_hpi) & !is.na(dlog_yoy_regional_pci)] %>%
    .[, index := as.Date(paste0(year, "-12-01"))] 

  dt_est_data[dt_hm_cycles, 
              on = .(index >= cycle_start_date, index <= cycle_end_date), 
              hm_cycle := i.cycle_label]

  if (nrow(dt_est_data[is.na(hm_cycle)]) > 0) {
    n_miss <- nrow(dt_est_data[is.na(hm_cycle)])
    warning(sprintf("Warning: %d rows in FHFA data did not match any Housing Cycle date range.", n_miss))
  }
  
  dt_est_data <- dt_est_data[!is.na(hm_cycle)]
  dt_est_data[, index_char := as.character(year)]

  dt_est_data[, dlog_yoy_fhfa_cbsa_hpi_resid := felm(
      dlog_yoy_fhfa_cbsa_hpi ~ dlog_yoy_regional_pci:hm_cycle | index_char
    )$residuals]

  dt_input <- dt_est_data %>%
    .[, .(GEOID = cbsa, index, hp.target = dlog_yoy_fhfa_cbsa_hpi_resid)]

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "cbsa") %>%
    .[, year := year(index)] %>%
    .[, index := NULL]

  dt_out <- merge(dt_est_data, dt_lu_ml, by = c("cbsa", "year"), all.x = TRUE) %>%
    select_by_ref(c("cbsa", "year", "cbsa_name", "cz20", "hm_cycle", 
                    "dlog_yoy_regional_pci", "fhfa_cbsa_hpi", 
                    "dlog_yoy_fhfa_cbsa_hpi", "dlog_yoy_fhfa_cbsa_hpi_resid", 
                    "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("cbsa", "year", "cbsa_name", "cz20", "hm_cycle", 
                  "dlog_yoy_regional_pci", "fhfa_cbsa_hpi", 
                  "dlog_yoy_fhfa_cbsa_hpi", "dlog_yoy_fhfa_cbsa_hpi_resid", 
                  "hp.target", "lu_ml_xgboost"))
    
  return(dt_out)
}

f_get_fhfa_county_annual_lu_ml_panel <- function(dt_hp, file_path_lu, 
                                                 dt_regional_pci_panel, 
                                                 dt_hm_cycles, dt_shp) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref], nanoparquet[read_parquet], 
    sf[st_is_empty], lfe[felm]
  )

  if (is.null(dt_shp)) stop("dt_shp is required for County level")

  dt_hp <- copy(dt_hp) %>% 
    .[, year := as.integer(year)] %>% 
    .[year >= 1980]

  dt_hp_cntys <- dt_hp[!duplicated(fips_code), .(fips_code)]
  dt_val <- merge(dt_hp_cntys, dt_shp, by.x = "fips_code", by.y = "GEOID", all.x = TRUE)
  if (nrow(dt_val[is.na(geometry) | st_is_empty(geometry)]) > 0) 
    stop("Error: County shapefile mismatch with FHFA data.")

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("GEOID", grep("unavailable", names(.), value = TRUE))) %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))]

  dt_pci <- copy(dt_regional_pci_panel) %>%
    .[, year := as.integer(year)] %>%
    setorder(target_geoid, year) %>%
    .[, dlog_yoy_regional_pci := log(regional_pci) - shift(log(regional_pci)), 
      by = target_geoid] %>%
    .[year >= 1980]

  dt_est_data <- merge(
    dt_hp, dt_pci[, .(fips_code = target_geoid, year, dlog_yoy_regional_pci)],
    by = c("fips_code", "year"), all.x = TRUE
  ) %>%
    .[!is.na(dlog_yoy_fhfa_cnty_hpi) & !is.na(dlog_yoy_regional_pci)] %>%
    .[, index := as.Date(paste0(year, "-12-01"))] 

  dt_est_data[dt_hm_cycles, 
              on = .(index >= cycle_start_date, index <= cycle_end_date), 
              hm_cycle := i.cycle_label]
  
  if (nrow(dt_est_data[is.na(hm_cycle)]) > 0) {
    n_miss <- nrow(dt_est_data[is.na(hm_cycle)])
    warning(sprintf("Warning: %d rows in FHFA County data did not match any Housing Cycle date range.", n_miss))
  }

  dt_est_data <- dt_est_data[!is.na(hm_cycle)]
  dt_est_data[, index_char := as.character(year)]
  
  dt_est_data[, dlog_yoy_fhfa_cnty_hpi_resid := felm(
      dlog_yoy_fhfa_cnty_hpi ~ dlog_yoy_regional_pci:hm_cycle | index_char
    )$residuals]

  dt_input <- dt_est_data %>%
    .[, .(GEOID = fips_code, index, hp.target = dlog_yoy_fhfa_cnty_hpi_resid)]

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "fips_code") %>%
    .[, year := year(index)] %>%
    .[, index := NULL]

  dt_out <- merge(dt_est_data, dt_lu_ml, by = c("fips_code", "year"), all.x = TRUE) %>%
    select_by_ref(c("fips_code", "year", "state", "cz20", "hm_cycle", 
                    "dlog_yoy_regional_pci", "fhfa_cnty_hpi", 
                    "dlog_yoy_fhfa_cnty_hpi", "dlog_yoy_fhfa_cnty_hpi_resid", 
                    "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("fips_code", "year", "state", "cz20", "hm_cycle", 
                  "dlog_yoy_regional_pci", "fhfa_cnty_hpi", 
                  "dlog_yoy_fhfa_cnty_hpi", "dlog_yoy_fhfa_cnty_hpi_resid", 
                  "hp.target", "lu_ml_xgboost"))

  return(dt_out)
}

f_get_fhfa_zip5_annual_lu_ml_panel <- function(dt_hp, file_path_lu,
                                               dt_regional_pci_panel,
                                               dt_hm_cycles) { 
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet], lfe[felm]
  )

  dt_hp <- copy(dt_hp) %>% 
    .[, year := as.integer(year)] %>% 
    .[year >= 1980]

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("zip5", grep("unavailable", names(.), value = TRUE))) %>%
    setnames("zip5", "GEOID") %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))]

  dt_pci <- copy(dt_regional_pci_panel) %>%
    .[, year := as.integer(year)] %>%
    setorder(target_geoid, year) %>%
    .[, dlog_yoy_regional_pci := log(regional_pci) - shift(log(regional_pci)),
      by = target_geoid] %>%
    .[year >= 1980]

  dt_est_data <- merge(
    dt_hp, dt_pci[, .(zip5 = target_geoid, year, dlog_yoy_regional_pci)],
    by = c("zip5", "year"), all.x = TRUE
  ) %>%
    .[!is.na(dlog_yoy_fhfa_zip5_hpi) & !is.na(dlog_yoy_regional_pci)] %>%
    .[, index := as.Date(paste0(year, "-12-01"))] 

  dt_est_data[dt_hm_cycles, 
              on = .(index >= cycle_start_date, index <= cycle_end_date), 
              hm_cycle := i.cycle_label]

  if (nrow(dt_est_data[is.na(hm_cycle)]) > 0) {
    n_miss <- nrow(dt_est_data[is.na(hm_cycle)])
    warning(sprintf("Warning: %d rows in FHFA Zip5 data did not match any Housing Cycle date range.", n_miss))
  }

  dt_est_data <- dt_est_data[!is.na(hm_cycle)]
  dt_est_data[, index_char := as.character(year)]

  dt_est_data[, dlog_yoy_fhfa_zip5_hpi_resid := felm(
      dlog_yoy_fhfa_zip5_hpi ~ dlog_yoy_regional_pci:hm_cycle | index_char
    )$residuals]

  dt_input <- dt_est_data %>%
    .[, .(GEOID = zip5, index, hp.target = dlog_yoy_fhfa_zip5_hpi_resid)]

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "zip5") %>%
    .[, year := year(index)] %>%
    .[, index := NULL]

  dt_out <- merge(dt_est_data, dt_lu_ml, by = c("zip5", "year"), all.x = TRUE) %>%
    select_by_ref(c("zip5", "cz20", "year", "hm_cycle", "dlog_yoy_regional_pci",
                    "fhfa_zip5_hpi", "dlog_yoy_fhfa_zip5_hpi",
                    "dlog_yoy_fhfa_zip5_hpi_resid", "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("zip5", "cz20", "year", "hm_cycle", "dlog_yoy_regional_pci",
                  "fhfa_zip5_hpi", "dlog_yoy_fhfa_zip5_hpi",
                  "dlog_yoy_fhfa_zip5_hpi_resid", "hp.target", "lu_ml_xgboost"))

  return(dt_out)
}

f_get_fhfa_zip3_annual_lu_ml_panel <- function(dt_hp, file_path_lu, 
                                               dt_regional_pci_panel,
                                               dt_hm_cycles) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet], lfe[felm]
  )

  dt_hp <- copy(dt_hp) %>% 
    .[, year := as.integer(year)] %>% 
    .[year >= 1980]

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("zip3", grep("unavailable", names(.), value = TRUE))) %>%
    setnames("zip3", "GEOID") %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))]

  dt_pci <- copy(dt_regional_pci_panel) %>%
    .[, year := as.integer(year)] %>%
    setorder(target_geoid, year) %>%
    .[, dlog_yoy_regional_pci := log(regional_pci) - shift(log(regional_pci)),
      by = target_geoid] %>%
    .[year >= 1980]

  dt_est_data <- merge(
    dt_hp, dt_pci[, .(zip3 = target_geoid, year, dlog_yoy_regional_pci)],
    by = c("zip3", "year"), all.x = TRUE
  ) %>%
    .[!is.na(dlog_yoy_fhfa_zip3_hpi) & !is.na(dlog_yoy_regional_pci)] %>%
    .[, index := as.Date(paste0(year, "-12-01"))]

  dt_est_data[dt_hm_cycles, 
              on = .(index >= cycle_start_date, index <= cycle_end_date), 
              hm_cycle := i.cycle_label]

  if (nrow(dt_est_data[is.na(hm_cycle)]) > 0) {
    n_miss <- nrow(dt_est_data[is.na(hm_cycle)])
    warning(sprintf("Warning: %d rows in FHFA Zip3 data did not match any Housing Cycle date range.", n_miss))
  }

  dt_est_data <- dt_est_data[!is.na(hm_cycle)]
  dt_est_data[, index_char := as.character(year)]

  dt_est_data[, dlog_yoy_fhfa_zip3_hpi_resid := felm(
      dlog_yoy_fhfa_zip3_hpi ~ dlog_yoy_regional_pci:hm_cycle | index_char
    )$residuals]

  dt_input <- dt_est_data %>%
    .[, .(GEOID = zip3, index, hp.target = dlog_yoy_fhfa_zip3_hpi_resid)]

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "zip3") %>%
    .[, year := year(index)] %>%
    .[, index := NULL]

  dt_out <- merge(dt_est_data, dt_lu_ml, by = c("zip3", "year"), all.x = TRUE) %>%
    select_by_ref(c("zip3", "cz20", "year", "hm_cycle", "dlog_yoy_regional_pci",
                    "fhfa_zip3_hpi", "dlog_yoy_fhfa_zip3_hpi",
                    "dlog_yoy_fhfa_zip3_hpi_resid", "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("zip3", "cz20", "year", "hm_cycle", "dlog_yoy_regional_pci",
                  "fhfa_zip3_hpi", "dlog_yoy_fhfa_zip3_hpi",
                  "dlog_yoy_fhfa_zip3_hpi_resid", "hp.target", "lu_ml_xgboost"))

  return(dt_out)
}

f_get_fhfa_tract_annual_lu_ml_panel <- function(dt_hp, file_path_lu, 
                                                dt_regional_pci_panel,
                                                dt_hm_cycles,
                                                dt_shp, dt_tract_cw) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet], sf[st_is_empty], lfe[felm]
  )
  
  if (is.null(dt_shp)) stop("dt_shp is required for Tract level")
  if (is.null(dt_tract_cw)) stop("dt_tract_cw is required for Tract level")

  dt_hp <- copy(dt_hp) %>% 
    .[, year := as.integer(year)] %>% 
    .[year >= 1980]

  dt_tract_cw <- copy(dt_tract_cw) %>%
    .[!is.na(TRACT2020)] %>% 
    .[, tract := paste0(STATEFP, COUNTYFP, TRACT2020)] %>%
    select_by_ref(c("NHGISCODE", "GJOIN2020", "tract"))

  dt_hp_tracts <- dt_hp[!duplicated(tract), .(tract)]

  dt_val <- merge(dt_hp_tracts, dt_shp, by.x = "tract", by.y = "GEOID", all.x = TRUE)
  if (nrow(dt_val[is.na(geometry) | st_is_empty(geometry)]) > 0) 
    stop("Error: Tract shapefile mismatch with FHFA data.")

  dt_val_cw <- merge(dt_hp_tracts, dt_tract_cw, by = "tract", all.x = TRUE)
  if (nrow(dt_val_cw[is.na(NHGISCODE)]) > 0)
    stop("Error: NHGIS crosswalk mismatch with FHFA data.")

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("NHGISCODE", grep("unavailable", names(.), value = TRUE))) %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))] %>%
    merge(dt_tract_cw, by = "NHGISCODE") %>%
    setnames("tract", "GEOID") %>%
    select_by_ref(c("GEOID", grep("unavailable", names(.), value = TRUE))) %>%
    setcolorder(c("GEOID", setdiff(names(.), "GEOID")))

  dt_pci <- copy(dt_regional_pci_panel) %>%
    .[, year := as.integer(year)] %>%
    setorder(target_geoid, year) %>%
    .[, dlog_yoy_regional_pci := log(regional_pci) - shift(log(regional_pci)),
      by = target_geoid] %>%
    .[year >= 1980]

  dt_est_data <- merge(
    dt_hp, dt_pci[, .(tract = target_geoid, year, dlog_yoy_regional_pci)],
    by = c("tract", "year"), all.x = TRUE
  ) %>%
    .[!is.na(dlog_yoy_fhfa_trct_hpi) & !is.na(dlog_yoy_regional_pci)] %>%
    .[, index := as.Date(paste0(year, "-12-01"))]

  dt_est_data[dt_hm_cycles, 
              on = .(index >= cycle_start_date, index <= cycle_end_date), 
              hm_cycle := i.cycle_label]

  if (nrow(dt_est_data[is.na(hm_cycle)]) > 0) {
    n_miss <- nrow(dt_est_data[is.na(hm_cycle)])
    warning(sprintf("Warning: %d rows in FHFA Tract data did not match any Housing Cycle date range.", n_miss))
  }

  dt_est_data <- dt_est_data[!is.na(hm_cycle)]
  dt_est_data[, index_char := as.character(year)]

  dt_est_data[, dlog_yoy_fhfa_trct_hpi_resid := felm(
      dlog_yoy_fhfa_trct_hpi ~ dlog_yoy_regional_pci:hm_cycle | index_char
    )$residuals]

  dt_input <- dt_est_data %>%
    .[, .(GEOID = tract, index, hp.target = dlog_yoy_fhfa_trct_hpi_resid)]

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "tract") %>%
    .[, year := year(index)] %>%
    .[, index := NULL]

  dt_out <- merge(dt_est_data, dt_lu_ml, by = c("tract", "year"), all.x = TRUE) %>%
    merge(dt_tract_cw, by = "tract", all.x = TRUE) %>%
    setnames("tract", "tract2020") %>% 
    select_by_ref(c("NHGISCODE", "GJOIN2020", "tract2020", "cz20", "year", "hm_cycle",
                    "dlog_yoy_regional_pci", "fhfa_trct_hpi", "dlog_yoy_fhfa_trct_hpi",
                    "dlog_yoy_fhfa_trct_hpi_resid", "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("NHGISCODE", "GJOIN2020", "tract2020", "cz20", "year", "hm_cycle",
                    "dlog_yoy_regional_pci", "fhfa_trct_hpi", "dlog_yoy_fhfa_trct_hpi",
                    "dlog_yoy_fhfa_trct_hpi_resid", "hp.target", "lu_ml_xgboost"))

  return(dt_out)
}


f_get_zillow_cbsa_zhvi_lu_ml_panel <- function(dt_hp, file_path_lu, 
                                               dt_regional_pci_panel, 
                                               dt_hm_cycles,
                                               dt_shp) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref], nanoparquet[read_parquet],
    sf[st_is_empty], lfe[felm]
  )

  if (is.null(dt_shp)) stop("dt_shp is required for Metro/CBSA level")

  dt_hp <- copy(dt_hp) %>%
    .[!is.na(zhvi)] %>%
    .[RegionType == "msa"] %>%
    .[!is.na(cbsa)] %>%
    setorder(cbsa, index) %>%
    .[, year := year(index)] %>%
    .[, dlog_yoy_metro_zhvi := log(zhvi) - log(shift(zhvi, 12)), by = cbsa]

  if ("cbsa_yr" %in% names(dt_hp)) {
    if (length(unique(dt_hp$cbsa_yr)) > 1) {
      stop("Error: Input data contains mixed 'cbsa_yr' definitions.")
    }
  }

  dt_hp_cbsas <- dt_hp[!duplicated(cbsa), .(cbsa, RegionName)] 
  
  dt_val <- merge(dt_hp_cbsas, dt_shp, by.x = "cbsa", by.y = "GEOID", all.x = TRUE)
  if (nrow(dt_val[is.na(geometry) | st_is_empty(geometry)]) > 0) 
    stop("Error: CBSA shapefile mismatch with Zillow Metro data.")

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("GEOID", grep("unavailable", names(.), value = TRUE))) %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))]

  dt_val_lu <- merge(dt_hp_cbsas[!grepl("AK$|HI$|PR$", RegionName)], dt_lu,
                     by.x = "cbsa", by.y = "GEOID", all.x = TRUE)

  check_col <- grep("unavailable", names(dt_lu), value = TRUE)[1]
  if (nrow(dt_val_lu[is.na(get(check_col))]) > 0) 
    stop("Error: LU data mismatch with Zillow Metro data.")

  dt_pci <- copy(dt_regional_pci_panel) %>%
    .[, year := as.integer(year)] %>%
    setorder(target_geoid, year) %>%
    .[, dlog_yoy_regional_pci := log(regional_pci) - shift(log(regional_pci)), 
      by = target_geoid] %>%
    .[year >= 2000]

  dt_est_data <- merge(
    dt_hp, dt_pci[, .(cbsa = target_geoid, year, dlog_yoy_regional_pci)],
    by = c("cbsa", "year"), all.x = TRUE
  ) %>%
    .[!is.na(dlog_yoy_metro_zhvi) & !is.na(dlog_yoy_regional_pci)]

  dt_est_data[dt_hm_cycles, 
              on = .(index >= cycle_start_date, index <= cycle_end_date), 
              hm_cycle := i.cycle_label]

  if (nrow(dt_est_data[is.na(hm_cycle)]) > 0) {
    n_miss <- nrow(dt_est_data[is.na(hm_cycle)])
    warning(sprintf("Warning: %d rows in Zillow CBSA data did not match any Housing Cycle date range.", n_miss))
  }

  dt_est_data <- dt_est_data[!is.na(hm_cycle)]
  dt_est_data[, index_char := as.character(year)]

  dt_est_data[, dlog_yoy_metro_zhvi_resid := felm(
      dlog_yoy_metro_zhvi ~ dlog_yoy_regional_pci:hm_cycle | index_char
    )$residuals]

  dt_input <- dt_est_data %>%
    .[, .(GEOID = cbsa, index, hp.target = dlog_yoy_metro_zhvi_resid)]

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "cbsa")

  dt_out <- merge(dt_est_data, dt_lu_ml, by = c("cbsa", "index"), all.x = TRUE)

  cols_to_keep <- c("cbsa", "cbsaname", "cz20", "RegionID", "StateName", "RegionName",
                    "index", "year", "hm_cycle",
                    "dlog_yoy_regional_pci", "zhvi", "dlog_yoy_metro_zhvi",
                    "dlog_yoy_metro_zhvi_resid", "hp.target", "lu_ml_xgboost")
  
  dt_out <- dt_out %>%
    select_by_ref(cols_to_keep) %>%
    setcolorder(cols_to_keep)

  return(dt_out)
}

f_get_zillow_county_zhvi_lu_ml_panel <- function(dt_hp, file_path_lu, 
                                                 dt_regional_pci_panel, 
                                                 dt_hm_cycles,
                                                 dt_shp) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet], sf[st_is_empty], lfe[felm]
  )

  if (is.null(dt_shp)) stop("dt_shp is required for County level")

  dt_hp <- copy(dt_hp) %>%
    .[!is.na(zhvi)] %>%
    .[, cntyfp := paste0(StateCodeFIPS, MunicipalCodeFIPS)] %>%
    setnames("RegionName", "cntynm") %>% 
    select_by_ref(c("cntyfp", "cntynm", "cz20", "index", "zhvi")) %>%
    setcolorder(c("cntyfp", "cntynm", "cz20", "index", "zhvi")) %>%
    setorder(cntyfp, index) %>%
    .[, year := year(index)] %>%
    .[, dlog_yoy_cnty_zhvi := log(zhvi) - log(shift(zhvi, 12)), by = cntyfp]

  dt_hp_cntys <- dt_hp[!duplicated(cntyfp), .(cntyfp)]
  
  dt_val <- merge(dt_hp_cntys, dt_shp, by.x = "cntyfp", by.y = "GEOID",
                  all.x = TRUE) %>%
    .[!grepl("^02|^15", cntyfp)] 
    
  if (nrow(dt_val[is.na(geometry) | st_is_empty(geometry)]) > 0) 
    stop("Error: County shapefile mismatch with Zillow data.")

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("GEOID", grep("unavailable", names(.), value = TRUE))) %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))]

  dt_val_lu <- merge(dt_hp_cntys[!grepl("^02|^15", cntyfp)], dt_lu,
                     by.x = "cntyfp", by.y = "GEOID", all.x = TRUE)
                      
  check_col <- grep("unavailable", names(dt_lu), value = TRUE)[1]
  if (nrow(dt_val_lu[is.na(get(check_col))]) > 0) 
    stop("Error: LU data mismatch with Zillow County data (missing LU features for some counties).")

  dt_pci <- copy(dt_regional_pci_panel) %>%
    .[, year := as.integer(year)] %>%
    setorder(target_geoid, year) %>%
    .[, dlog_yoy_regional_pci := log(regional_pci) - shift(log(regional_pci)), 
      by = target_geoid] %>%
    .[year >= 2000]

  dt_est_data <- merge(
    dt_hp, dt_pci[, .(cntyfp = target_geoid, year, dlog_yoy_regional_pci)],
    by = c("cntyfp", "year"), all.x = TRUE
  ) %>%
    .[!is.na(dlog_yoy_cnty_zhvi) & !is.na(dlog_yoy_regional_pci)]

  dt_est_data[dt_hm_cycles, 
              on = .(index >= cycle_start_date, index <= cycle_end_date), 
              hm_cycle := i.cycle_label]

  if (nrow(dt_est_data[is.na(hm_cycle)]) > 0) {
    n_miss <- nrow(dt_est_data[is.na(hm_cycle)])
    warning(sprintf("Warning: %d rows in Zillow County data did not match any Housing Cycle date range.", n_miss))
  }

  dt_est_data <- dt_est_data[!is.na(hm_cycle)]
  dt_est_data[, index_char := as.character(year)]

  dt_est_data[, dlog_yoy_cnty_zhvi_resid := felm(
      dlog_yoy_cnty_zhvi ~ dlog_yoy_regional_pci:hm_cycle | index_char
    )$residuals]

  dt_input <- dt_est_data %>%
    .[, .(GEOID = cntyfp, index, hp.target = dlog_yoy_cnty_zhvi_resid)]

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "cntyfp")

  dt_out <- merge(dt_est_data, dt_lu_ml, by = c("cntyfp", "index"), all.x = TRUE) %>%
    select_by_ref(c("cntyfp", "cntynm", "cz20", "index", "hm_cycle", 
                    "dlog_yoy_regional_pci", "zhvi", "dlog_yoy_cnty_zhvi",
                    "dlog_yoy_cnty_zhvi_resid", "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("cntyfp", "cntynm", "cz20", "index", "hm_cycle", 
                    "dlog_yoy_regional_pci", "zhvi", "dlog_yoy_cnty_zhvi",
                    "dlog_yoy_cnty_zhvi_resid", "hp.target", "lu_ml_xgboost"))

  return(dt_out)
}

f_get_zillow_zip5_zhvi_lu_ml_panel <- function(dt_hp, file_path_lu, 
                                               dt_regional_pci_panel,
                                               dt_hm_cycles) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet], lfe[felm]
  )

  dt_hp <- copy(dt_hp) %>%
    .[!is.na(zhvi)] %>%
    .[, zip := RegionName] %>%
    select_by_ref(c("zip", "cz20", "index", "zhvi")) %>%
    setcolorder(c("zip", "cz20", "index", "zhvi")) %>%
    setorder(zip, index) %>%
    .[, year := year(index)] %>%
    .[, dlog_yoy_zip5_zhvi := log(zhvi) - log(shift(zhvi, 12)), by = zip]

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("zip5", grep("unavailable", names(.), value = TRUE))) %>%
    setnames("zip5", "GEOID") %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))]

  dt_pci <- copy(dt_regional_pci_panel) %>%
    .[, year := as.integer(year)] %>%
    setorder(target_geoid, year) %>%
    .[, dlog_yoy_regional_pci := log(regional_pci) - shift(log(regional_pci)), 
      by = target_geoid] %>%
    .[year >= 2000]

  dt_est_data <- merge(
    dt_hp, dt_pci[, .(zip = target_geoid, year, dlog_yoy_regional_pci)],
    by = c("zip", "year"), all.x = TRUE
  ) %>%
    .[!is.na(dlog_yoy_zip5_zhvi) & !is.na(dlog_yoy_regional_pci)]

  dt_est_data[dt_hm_cycles, 
              on = .(index >= cycle_start_date, index <= cycle_end_date), 
              hm_cycle := i.cycle_label]

  if (nrow(dt_est_data[is.na(hm_cycle)]) > 0) {
    n_miss <- nrow(dt_est_data[is.na(hm_cycle)])
    warning(sprintf("Warning: %d rows in Zillow Zip5 data did not match any Housing Cycle date range.", n_miss))
  }

  dt_est_data <- dt_est_data[!is.na(hm_cycle)]
  dt_est_data[, index_char := as.character(year)]

  dt_est_data[, dlog_yoy_zip5_zhvi_resid := felm(
      dlog_yoy_zip5_zhvi ~ dlog_yoy_regional_pci:hm_cycle | index_char
    )$residuals]

  dt_input <- dt_est_data %>%
    .[, .(GEOID = zip, index, hp.target = dlog_yoy_zip5_zhvi_resid)]

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "zip") 
  
  dt_out <- merge(dt_est_data, dt_lu_ml, by = c("zip", "index"), all.x = TRUE) %>%
    select_by_ref(c("zip", "cz20", "index", "hm_cycle", "dlog_yoy_regional_pci",
                    "zhvi", "dlog_yoy_zip5_zhvi", "dlog_yoy_zip5_zhvi_resid",
                    "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("zip", "cz20", "index", "hm_cycle", "dlog_yoy_regional_pci",
                  "zhvi", "dlog_yoy_zip5_zhvi", "dlog_yoy_zip5_zhvi_resid",
                  "hp.target", "lu_ml_xgboost")) 

  return(dt_out)
}

f_get_fhfa_zip3_qtrly_lu_ml_panel <- function(dt_hp, file_path_lu, 
                                              dt_regional_pci_panel,
                                              dt_hm_cycles) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet], lfe[felm]
  )

  dt_hp <- copy(dt_hp) %>%
    .[, year := year(index)] %>%
    .[year >= 1980]

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("zip3", grep("unavailable", names(.), value = TRUE))) %>%
    setnames("zip3", "GEOID") %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))]

  dt_pci <- copy(dt_regional_pci_panel) %>%
    .[, year := as.integer(year)] %>%
    setorder(target_geoid, year) %>%
    .[, dlog_yoy_regional_pci := log(regional_pci) - shift(log(regional_pci)), 
      by = target_geoid] %>%
    .[year >= 1980]

  dt_est_data <- merge(
    dt_hp, dt_pci[, .(zip3 = target_geoid, year, dlog_yoy_regional_pci)],
    by = c("zip3", "year"), all.x = TRUE
  ) %>%
    .[!is.na(dlog_yoy_hpi) & !is.na(dlog_yoy_regional_pci)]

  dt_est_data[dt_hm_cycles, 
              on = .(index >= cycle_start_date, index <= cycle_end_date), 
              hm_cycle := i.cycle_label]

  if (nrow(dt_est_data[is.na(hm_cycle)]) > 0) {
    n_miss <- nrow(dt_est_data[is.na(hm_cycle)])
    warning(sprintf("Warning: %d rows in FHFA Zip3 Quarterly data did not match any Housing Cycle date range.", n_miss))
  }

  dt_est_data <- dt_est_data[!is.na(hm_cycle)]
  dt_est_data[, index_char := as.character(index)]

  dt_est_data[, dlog_yoy_hpi_resid := felm(
      dlog_yoy_hpi ~ dlog_yoy_regional_pci:hm_cycle | index_char
    )$residuals]

  dt_input <- dt_est_data %>%
    .[, .(GEOID = zip3, index, hp.target = dlog_yoy_hpi_resid)]

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "zip3") 

  dt_out <- merge(dt_est_data, dt_lu_ml, by = c("zip3", "index"), all.x = TRUE) %>%
    select_by_ref(c("zip3", "index", "hm_cycle", "dlog_yoy_regional_pci", "hpi", 
                    "dlog_yoy_hpi", "dlog_yoy_hpi_resid",
                    "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("zip3", "index", "hm_cycle", "dlog_yoy_regional_pci", "hpi", 
                  "dlog_yoy_hpi", "dlog_yoy_hpi_resid",
                  "hp.target", "lu_ml_xgboost"))

  return(dt_out)
}

f_get_mian_sufi_lu_ml <- function(file_path_ms, file_path_lu, dt_regional_pci_panel) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet], lfe[felm]
  )
  
  dt_ms <- readRDS(file_path_ms) %>%
    setDT() %>% 
    .[!is.na(house.net.worth)]
  
  dt_pci <- copy(dt_regional_pci_panel) %>%
    .[, year := as.integer(year)] %>%
    .[year %in% c(2006, 2009)] %>%
    .[order(target_geoid, year)] %>%
    .[, .(dlog_pci_06_09 = log(regional_pci[year == 2009]) - 
                           log(regional_pci[year == 2006])), 
      by = target_geoid] %>%
    .[!is.na(dlog_pci_06_09)]
  
  dt_est_data <- merge(dt_ms, dt_pci, by.x = "fips", by.y = "target_geoid",
                       all.x = TRUE) %>%
    .[!is.na(house.net.worth) & !is.na(dlog_pci_06_09)]
  
  dt_est_data[, house_net_worth_resid := lm(
    house.net.worth ~ dlog_pci_06_09
  )$residuals]
  
  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("GEOID", grep("unavailable", names(.), value = TRUE))) %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))] %>%
    .[order(GEOID)]
  
  dt_input <- dt_est_data %>%
    .[, .(GEOID = fips, index = as.Date("2006-01-01"),
          hp.target = house_net_worth_resid)]
  
  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = FALSE) %>%
    setnames("GEOID", "fips") %>%
    .[, index := NULL]
  
  dt_out <- merge(dt_est_data, dt_lu_ml, by = "fips", all.x = TRUE)
  
  return(dt_out)
}


f_get_guren_et_al_lu_ml <- function(file_path_guren, file_path_lu, 
                                    dt_regional_pci_panel, dt_hm_cycles) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    fst[read_fst], nanoparquet[read_parquet], lfe[felm]
  )

  dt_pci <- copy(dt_regional_pci_panel) %>%
    setnames("target_geoid", "cbsa") %>%
    .[, year := as.integer(year)] %>%
    setorder(cbsa, year) %>%
    .[, dlog_yoy_regional_pci := log(regional_pci) - shift(log(regional_pci)),
      by = cbsa] %>%
    .[!is.na(dlog_yoy_regional_pci)]

  dt_est <- read_fst(
    file_path_guren,
    columns = c("cbsa", "date", "lhpi_a"), 
    as.data.table = TRUE
  ) %>%
    .[, cbsa := as.character(cbsa)] %>%
    .[, year := year(date)] %>% 
    setnames("date", "index")

  dt_est <- merge(dt_est, dt_pci, by = c("cbsa", "year"), all.x = TRUE)

  dt_est[dt_hm_cycles, 
         on = .(index >= cycle_start_date, index <= cycle_end_date), 
         hm_cycle := i.cycle_label]

  dt_est <- dt_est[!is.na(lhpi_a) & !is.na(dlog_yoy_regional_pci) & !is.na(hm_cycle)]

  dt_est[, index_char := as.character(index)]
  
  dt_est[, lhpi_a_resid := felm(
      lhpi_a ~ dlog_yoy_regional_pci:hm_cycle | index_char
    )$residuals]

  dt_input <- dt_est %>%
    .[, .(GEOID = cbsa, index, hp.target = lhpi_a_resid)]

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("GEOID", grep("unavailable", names(.), value = TRUE))) %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))] %>%
    setorder(GEOID)

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "cbsa")
  
  dt_out <- merge(dt_est, dt_lu_ml, by = c("cbsa", "index"), all.x = TRUE) %>%
    select_by_ref(c("cbsa", "index", "year", "hm_cycle", 
                    "lhpi_a", "dlog_yoy_regional_pci", 
                    "lhpi_a_resid", "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("cbsa", "index", "year", "hm_cycle", 
                  "lhpi_a", "dlog_yoy_regional_pci", 
                  "lhpi_a_resid", "hp.target", "lu_ml_xgboost"))

  return(dt_out)
}

f_get_chaney_et_al_lu_ml <- function(file_path_chaneyetal, file_path_lu, 
                                     dt_regional_pci_panel, dt_hm_cycles) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet], lfe[felm]
  )

  dt_pci <- copy(dt_regional_pci_panel) %>%
    setnames("target_geoid", "msacode") %>%
    .[, year := as.integer(year)] %>%
    .[, msacode := as.character(msacode)] %>%
    .[, log_regional_pci := log(regional_pci)] %>%
    select_by_ref(c("msacode", "year", "log_regional_pci"))

  dt_est <- read_parquet(file_path_chaneyetal) %>%
    setDT() %>%
    .[, id := as.character(.GRP), by = .(msacode, msacode_orig, msa)] %>%
    .[year >= 1993] %>%
    .[, msacode := as.character(msacode)] %>%
    .[, index := as.Date(paste0(year, "-01-01"))]
  
  dt_est <- merge(dt_est, dt_pci, by = c("msacode", "year"), all.x = TRUE)

  dt_est[dt_hm_cycles, 
         on = .(index >= cycle_start_date, index <= cycle_end_date), 
         hm_cycle := i.cycle_label]

  dt_est <- dt_est[!is.na(index_normalized) & !is.na(log_regional_pci) &
                     !is.na(hm_cycle)]

  dt_est[, index_char := as.character(year)]

  dt_est[, index_normalized_resid := felm(
      index_normalized ~ log_regional_pci:hm_cycle | index_char
    )$residuals]

  dt_input <- dt_est %>%
    .[, .(GEOID = id, index, hp.target = index_normalized_resid)]

  dt_lu_base <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("GEOID", grep("unavailable", names(.), value = TRUE))) %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))] 

  dt_map <- unique(dt_est[, .(id, msacode)])

  dt_lu <- merge(dt_map, dt_lu_base, by.x = "msacode", by.y = "GEOID") %>%
    .[, msacode := NULL] %>%
    setnames("id", "GEOID") %>%
    setorder(GEOID)

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "id")

  dt_out <- merge(dt_est, dt_lu_ml, by = c("id", "index"), all.x = TRUE) %>%
    select_by_ref(c("id", "msacode", "year", "hm_cycle", 
                    "index_normalized", "log_regional_pci", 
                    "index_normalized_resid", "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("id", "msacode", "year", "hm_cycle", 
                  "index_normalized", "log_regional_pci", 
                  "index_normalized_resid", "hp.target", "lu_ml_xgboost"))

  return(dt_out)
}

f_get_stroebel_vavra_01_06_lu_ml <- function(file_path_hp, file_path_lu, 
                                             dt_regional_pci_panel) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet]
  )

  dt_pci <- copy(dt_regional_pci_panel) %>%
    setnames("target_geoid", "cbsa") %>%
    .[, year := as.integer(year)] %>%
    .[year %in% c(2001, 2006)] %>%
    .[order(cbsa, year)] %>%
    .[, .(dlog_pci_01_06 = log(regional_pci[year == 2006]) - 
                           log(regional_pci[year == 2001])), 
      by = cbsa] %>%
    .[!is.na(dlog_pci_01_06)]

  dt_est <- read_parquet(file_path_hp) %>%
    setDT() %>%
    .[!is.na(d_index_sa)] %>%
    .[, .(cbsa = cbsa2023, d_index_sa)]

  dt_est <- merge(dt_est, dt_pci, by = "cbsa", all.x = TRUE) %>%
    .[!is.na(d_index_sa) & !is.na(dlog_pci_01_06)]

  dt_est[, d_index_sa_resid := lm(d_index_sa ~ dlog_pci_01_06)$residuals]

  dt_input <- dt_est %>%
    .[, .(GEOID = cbsa, index = as.Date("2001-01-01"), 
          hp.target = d_index_sa_resid)]

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("GEOID", grep("unavailable", names(.), value = TRUE))) %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))] 

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "cbsa") %>%
    .[, index := NULL] 

  dt_out <- merge(dt_est, dt_lu_ml, by = "cbsa", all.x = TRUE) %>%
    select_by_ref(c("cbsa", "d_index_sa", "dlog_pci_01_06", 
                    "d_index_sa_resid", "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("cbsa", "d_index_sa", "dlog_pci_01_06", 
                  "d_index_sa_resid", "hp.target", "lu_ml_xgboost"))

  return(dt_out)
}

f_get_stroebel_vavra_07_11_lu_ml <- function(file_path_hp, file_path_lu, 
                                             dt_regional_pci_panel) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet]
  )

  dt_pci <- copy(dt_regional_pci_panel) %>%
    setnames("target_geoid", "cbsa") %>%
    .[, year := as.integer(year)] %>%
    .[year %in% c(2007, 2011)] %>%
    .[order(cbsa, year)] %>%
    .[, .(dlog_pci_07_11 = log(regional_pci[year == 2011]) - 
                           log(regional_pci[year == 2007])), 
      by = cbsa] %>%
    .[!is.na(dlog_pci_07_11)]

  dt_est <- read_parquet(file_path_hp) %>%
    setDT() %>%
    .[!is.na(d_index_sa)] %>%
    .[, .(cbsa = cbsa2023, d_index_sa)]

  dt_est <- merge(dt_est, dt_pci, by = "cbsa", all.x = TRUE) %>%
    .[!is.na(d_index_sa) & !is.na(dlog_pci_07_11)]

  dt_est[, d_index_sa_resid := lm(d_index_sa ~ dlog_pci_07_11)$residuals]

  dt_input <- dt_est %>%
    .[, .(GEOID = cbsa, index = as.Date("2007-01-01"), 
          hp.target = d_index_sa_resid)]

  dt_lu <- read_parquet(file_path_lu) %>%
    setDT() %>%
    select_by_ref(c("GEOID", grep("unavailable", names(.), value = TRUE))) %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))] 

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    setnames("GEOID", "cbsa") %>%
    .[, index := NULL]

  dt_out <- merge(dt_est, dt_lu_ml, by = "cbsa", all.x = TRUE) %>%
    select_by_ref(c("cbsa", "d_index_sa", "dlog_pci_07_11", 
                    "d_index_sa_resid", "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("cbsa", "d_index_sa", "dlog_pci_07_11", 
                  "d_index_sa_resid", "hp.target", "lu_ml_xgboost"))

  return(dt_out)
}
