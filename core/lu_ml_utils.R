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
  
  check_dups <- function(dt) {
    if (anyDuplicated(dt$GEOID) > 0) {
      stop(sprintf("Duplicate GEOIDs found for index: %s", unique(dt$index)[1]))
    }
  }
  
  f_wrkr <- function(dt_sub) {
    check_dups(dt_sub) 
    
    tryCatch({
      lu_ml_xgboost_time_varying(
        DT.hp = dt_sub,
        DT.lu = dt_lu,
        run_in_parallel = FALSE
      )
    }, error = function(cond) {
      current_index <- unique(dt_sub$index)[1]
      stop(sprintf("\n>>> XGBoost crashed on index: %s <<<\nOriginal Error: %s", 
                   current_index, conditionMessage(cond)), 
           call. = FALSE)
    })
  }
  
  if (run_in_parallel == FALSE) {
    
    dt_list <- split(dt_est, by = "index")
    
    dt_res <- lapply(dt_list, f_wrkr) %>%
      rbindlist(use.names = TRUE)
      
  } else if (run_in_parallel == TRUE && n_indices == 1) {
    
    dt_res <- f_wrkr(dt_est)
    
  } else if (run_in_parallel == TRUE && n_indices > 1) {
    
    dt_list <- split(dt_est, by = "index")
    
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

f_prep_lu_data <- function(file_path_lu, geog_id_col = "GEOID", dt_tract_cw = NULL) {
  
  box::use(
    data.table[...], magrittr[`%>%`], nanoparquet[read_parquet], CLmisc[select_by_ref]
  )

  dt_lu <- read_parquet(file_path_lu) %>% setDT()

  if (!is.null(dt_tract_cw) && geog_id_col == "NHGISCODE") {
    dt_tract_cw <- copy(dt_tract_cw)[!is.na(TRACT2020)] %>%
      .[, tract := paste0(STATEFP, COUNTYFP, TRACT2020)]
    
    dt_lu <- merge(dt_lu, dt_tract_cw, by = "NHGISCODE", all.x = TRUE) %>%
      .[!is.na(tract)]
    geog_id_col <- "tract" 
  }

  dt_lu <- dt_lu %>%
    select_by_ref(c(geog_id_col, grep("unavailable", names(.), value = TRUE))) %>%
    .[, names(.SD) := NULL, .SDcols = grepl("^total_unavailable", names(.))]
  
  if (geog_id_col != "GEOID") {
    dt_lu <- setnames(dt_lu, geog_id_col, "GEOID")
  }

  dt_lu <- dt_lu[, GEOID := as.character(GEOID)] %>%
    setcolorder("GEOID") %>%
    setorder(GEOID)

  return(dt_lu)
}

f_get_universal_lu_ml_panel <- function(dt_hp, dt_lu, dt_regional_pci_panel,
                                        dt_gmaps_amenity_demand, dt_hm_cycles) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    fixest[feols, xpd]
  )

  gmaps_amenity_demand_cols <- names(dt_gmaps_amenity_demand) %>%
    .[grepl("demand_index", .) & !grepl("nature", ., ignore.case = TRUE)]

  dt_gmaps_amenity_demand <- copy(dt_gmaps_amenity_demand) %>%
    select_by_ref(c("target_geoid", gmaps_amenity_demand_cols)) %>%
    setnames("target_geoid", "GEOID")

  dt_pci <- copy(dt_regional_pci_panel) %>%
    setnames("target_geoid", "GEOID", skip_absent = TRUE) %>%
    .[, year := as.integer(year)] %>%
    setorder(GEOID, year) %>%
    .[, dlog_yoy_regional_pci := log(regional_pci) - shift(log(regional_pci)),
      by = GEOID]

  dt_est_data <- copy(dt_hp) %>%
    .[, year := as.integer(year(index))] %>%
    merge(dt_pci[, .(GEOID, year, dlog_yoy_regional_pci)], by = c("GEOID", "year"),
          all.x = TRUE) %>%
    .[!is.na(dlog_yoy_hp_local) & !is.na(dlog_yoy_regional_pci)]

  dt_est_data[dt_hm_cycles, 
              on = .(index >= cycle_start_date, index <= cycle_end_date), 
              hm_cycle := i.cycle_label]

  if (nrow(dt_est_data[is.na(hm_cycle)]) > 0) {
    n_miss <- nrow(dt_est_data[is.na(hm_cycle)])
    warning(sprintf("Warning: %d rows did not match any Housing Cycle date range.",
                    n_miss))
  }

  dt_est_data <- dt_est_data[!is.na(hm_cycle)] %>%
    merge(dt_gmaps_amenity_demand, by = "GEOID", all.x = TRUE) %>%
    na.omit(cols = c("dlog_yoy_hp_local", "dlog_yoy_regional_pci", 
                     "hm_cycle", gmaps_amenity_demand_cols))
  
  dt_est_data[, index_char := as.character(index)]

  interaction_terms <- paste0(gmaps_amenity_demand_cols, ":hm_cycle")

  f_formula <- xpd(dlog_yoy_hp_local ~ dlog_yoy_regional_pci:hm_cycle +
                     ..interactions | index_char,
                   ..interactions = interaction_terms)

  first_stage_model <- feols(f_formula, data = dt_est_data)

  dt_est_data[, hp.target := residuals(first_stage_model)]

  dt_input <- dt_est_data[, .(GEOID, index, hp.target)]

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = TRUE) %>%
    select_by_ref(c("GEOID", "index", "lu_ml_xgboost"))

  dt_out <- merge(dt_est_data, dt_lu_ml, by = c("GEOID", "index"), all.x = TRUE) %>%
    .[, index_char := NULL] 

  return(dt_out)
}


f_get_mian_sufi_lu_ml <- function(file_path_ms, dt_lu, dt_regional_pci_panel, 
                                  dt_gmaps_amenity_demand) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    fixest[feols, xpd]
  )
  
  gmaps_amenity_demand_cols <- names(dt_gmaps_amenity_demand) %>%
    .[grepl("demand_index", .) & !grepl("nature", ., ignore.case = TRUE)]
  
  dt_gmaps_amenity_demand <- copy(dt_gmaps_amenity_demand) %>%
    select_by_ref(c("target_geoid", gmaps_amenity_demand_cols)) %>%
    setnames("target_geoid", "fips")
  
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
    .[!is.na(dlog_pci_06_09)] %>%
    setnames("target_geoid", "fips")
  
  dt_est_data <- merge(dt_ms, dt_pci, by = "fips", all.x = TRUE) %>%
    .[!is.na(house.net.worth) & !is.na(dlog_pci_06_09)] %>%
    merge(dt_gmaps_amenity_demand, by = "fips") %>%
    na.omit(cols = c("house.net.worth", "dlog_pci_06_09", gmaps_amenity_demand_cols))
  
  f_formula <- xpd(house.net.worth ~ dlog_pci_06_09 + ..amenity_cols,
                   ..amenity_cols = gmaps_amenity_demand_cols)
  
  first_stage_model <- feols(f_formula, data = dt_est_data)
  
  dt_est_data <- dt_est_data %>%
    .[, house_net_worth_resid := residuals(first_stage_model)]
  
  dt_input <- dt_est_data %>%
    .[, .(GEOID = fips, index = as.Date("2006-01-01"),
          hp.target = house_net_worth_resid)]
  
  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = FALSE) %>%
    setnames("GEOID", "fips") %>%
    .[, index := NULL]
  
  dt_out <- merge(dt_est_data, dt_lu_ml, by = "fips", all.x = TRUE)
  
  return(dt_out)
}


f_get_guren_et_al_lu_ml <- function(file_path_guren, dt_lu, 
                                    dt_regional_pci_panel, 
                                    dt_gmaps_amenity_demand,
                                    dt_hm_cycles) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    fst[read_fst], fixest[feols, xpd]
  )

  gmaps_amenity_demand_cols <- names(dt_gmaps_amenity_demand) %>%
    .[grepl("demand_index", .) & !grepl("nature", ., ignore.case = TRUE)]

  dt_gmaps_amenity_demand <- copy(dt_gmaps_amenity_demand) %>%
    select_by_ref(c("target_geoid", gmaps_amenity_demand_cols)) %>%
    setnames("target_geoid", "cbsa") %>%
    .[, cbsa := as.character(cbsa)]

  dt_pci <- copy(dt_regional_pci_panel) %>%
    setnames("target_geoid", "cbsa") %>%
    .[, year := as.integer(year)] %>%
    .[, cbsa := as.character(cbsa)] %>%
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

  dt_est <- merge(dt_est, dt_gmaps_amenity_demand, by = "cbsa", all.x = TRUE) %>%
    na.omit(cols = c("lhpi_a", "dlog_yoy_regional_pci", "hm_cycle",
                     gmaps_amenity_demand_cols))

  dt_est[, index_char := as.character(index)]

  interaction_terms <- paste0(gmaps_amenity_demand_cols, ":hm_cycle")

  f_formula <- xpd(lhpi_a ~ dlog_yoy_regional_pci:hm_cycle +
                     ..interactions | index_char,
                   ..interactions = interaction_terms)

  first_stage_model <- feols(f_formula, data = dt_est)

  dt_est[, lhpi_a_resid := residuals(first_stage_model)]

  dt_input <- dt_est %>%
    .[, .(GEOID = cbsa, index, hp.target = lhpi_a_resid)]

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


f_get_chaney_et_al_lu_ml <- function(file_path_chaneyetal, dt_lu_base, 
                                     dt_regional_pci_panel, 
                                     dt_gmaps_amenity_demand,
                                     dt_hm_cycles) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet], fixest[feols, xpd]
  )

  gmaps_amenity_demand_cols <- names(dt_gmaps_amenity_demand) %>%
    .[grepl("demand_index", .) & !grepl("nature", ., ignore.case = TRUE)]

  dt_gmaps_amenity_demand <- copy(dt_gmaps_amenity_demand) %>%
    select_by_ref(c("target_geoid", gmaps_amenity_demand_cols)) %>%
    setnames("target_geoid", "msacode") %>%
    .[, msacode := as.character(msacode)]

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

  dt_est <- merge(dt_est, dt_gmaps_amenity_demand, by = "msacode", all.x = TRUE) %>%
    na.omit(cols = c("index_normalized", "log_regional_pci", "hm_cycle",
                     gmaps_amenity_demand_cols))

  dt_est[, index_char := as.character(year)]

  interaction_terms <- paste0(gmaps_amenity_demand_cols, ":hm_cycle")

  f_formula <- xpd(index_normalized ~ log_regional_pci:hm_cycle +
                     ..interactions | index_char,
                   ..interactions = interaction_terms)

  first_stage_model <- feols(f_formula, data = dt_est)

  dt_est[, index_normalized_resid := residuals(first_stage_model)]

  dt_input <- dt_est %>%
    .[, .(GEOID = id, index, hp.target = index_normalized_resid)]

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

f_get_stroebel_vavra_01_06_lu_ml <- function(file_path_hp, dt_lu, 
                                             dt_regional_pci_panel,
                                             dt_gmaps_amenity_demand) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet], fixest[feols, xpd]
  )

  gmaps_amenity_demand_cols <- names(dt_gmaps_amenity_demand) %>%
    .[grepl("demand_index", .)]

  dt_gmaps_amenity_demand <- copy(dt_gmaps_amenity_demand) %>%
    select_by_ref(c("target_geoid", gmaps_amenity_demand_cols)) %>%
    setnames("target_geoid", "cbsa")

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

  dt_est <- merge(dt_est, dt_pci, by = "cbsa") %>%
    .[!is.na(d_index_sa) & !is.na(dlog_pci_01_06)] %>%
    merge(dt_gmaps_amenity_demand, by = "cbsa") %>%
    na.omit(cols = c("d_index_sa", "dlog_pci_01_06", gmaps_amenity_demand_cols))

  f_formula <- xpd(d_index_sa ~ dlog_pci_01_06 + ..amenity_cols,
                   ..amenity_cols = gmaps_amenity_demand_cols)

  first_stage_model <- feols(f_formula, data = dt_est)

  dt_est <- dt_est %>%
    .[, d_index_sa_resid := residuals(first_stage_model)]

  dt_input <- dt_est %>%
    .[, .(GEOID = cbsa, index = as.Date("2001-01-01"), 
          hp.target = d_index_sa_resid)]

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

f_get_stroebel_vavra_07_11_lu_ml <- function(file_path_hp, dt_lu, 
                                             dt_regional_pci_panel,
                                             dt_gmaps_amenity_demand) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref],
    nanoparquet[read_parquet], fixest[feols, xpd]
  )

  gmaps_amenity_demand_cols <- names(dt_gmaps_amenity_demand) %>%
    .[grepl("demand_index", .)]

  dt_gmaps_amenity_demand <- copy(dt_gmaps_amenity_demand) %>%
    select_by_ref(c("target_geoid", gmaps_amenity_demand_cols)) %>%
    setnames("target_geoid", "cbsa")

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

  dt_est <- merge(dt_est, dt_pci, by = "cbsa") %>%
    .[!is.na(d_index_sa) & !is.na(dlog_pci_07_11)] %>%
    merge(dt_gmaps_amenity_demand, by = "cbsa") %>%
    na.omit(cols = c("d_index_sa", "dlog_pci_07_11", gmaps_amenity_demand_cols))

  f_formula <- xpd(d_index_sa ~ dlog_pci_07_11 + ..amenity_cols,
                   ..amenity_cols = gmaps_amenity_demand_cols)

  first_stage_model <- feols(f_formula, data = dt_est)

  dt_est <- dt_est %>%
    .[, d_index_sa_resid := residuals(first_stage_model)]

  dt_input <- dt_est %>%
    .[, .(GEOID = cbsa, index = as.Date("2007-01-01"), 
          hp.target = d_index_sa_resid)]

  dt_lu_ml <- f_run_lu_ml(dt_input, dt_lu, run_in_parallel = FALSE) %>%
    setnames("GEOID", "cbsa") %>%
    .[, index := NULL]

  dt_out <- merge(dt_est, dt_lu_ml, by = "cbsa", all.x = TRUE) %>%
    select_by_ref(c("cbsa", "d_index_sa", "dlog_pci_07_11", 
                    "d_index_sa_resid", "hp.target", "lu_ml_xgboost")) %>%
    setcolorder(c("cbsa", "d_index_sa", "dlog_pci_07_11", 
                  "d_index_sa_resid", "hp.target", "lu_ml_xgboost"))

  return(dt_out)
}
