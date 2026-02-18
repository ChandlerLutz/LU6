## core/stage1_utils.R

f_prep_data_for_stage1_panel <- function(dt_lu_ml_panel, dt_natl_hp, dt_mtg, 
                                         dt_saiz, dt_bsh, dt_div) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref]
  )
  
  stopifnot(
    is.data.table(dt_lu_ml_panel),
    all(c("GEOID", "index", "hp.target", "lu_ml_xgboost", "cz20") %chin%
          names(dt_lu_ml_panel)),
    
    is.data.table(dt_natl_hp),
    all(c("index", "hp_natl", "dlog_yoy_hp_natl") %chin% names(dt_natl_hp)),
    
    is.data.table(dt_mtg),
    all(c("index", "real_mtg_rate", "d_yoy_real_mtg_rate") %chin% names(dt_mtg)),
    
    is.data.table(dt_saiz),
    all(c("GEOID", "saiz_elasticity") %chin% names(dt_saiz)),
    
    is.data.table(dt_bsh),
    all(c("GEOID_metro", "gamma01b_space_FMM") %chin% names(dt_bsh)),
    
    is.data.table(dt_div),
    all(c("geoid_cbsa_2023", "st_first", "census_region", "census_division") %chin%
          names(dt_div))
  )
  
  dt_div_clean <- copy(dt_div) %>%
    select_by_ref(c("geoid_cbsa_2023", "st_first", "census_region", 
                    "census_division")) %>%
    setnames("geoid_cbsa_2023", "cbsa")
  
  dt_natl_clean <- copy(dt_natl_hp) %>%
    select_by_ref(c("index", "hp_natl", "dlog_yoy_hp_natl"))
  
  dt_mtg_clean <- copy(dt_mtg) %>%
    select_by_ref(c("index", "real_mtg_rate", "d_yoy_real_mtg_rate"))
  
  dt_bsh_clean <- copy(dt_bsh) %>%
    .[, .(GEOID = GEOID_metro, gamma01b_space_FMM)]
  
  dt_saiz_clean <- copy(dt_saiz) %>%
    .[, .(GEOID = GEOID, saiz_elasticity)]
  
  dt_out <- copy(dt_lu_ml_panel) %>%
    merge(dt_div_clean, by.x = "GEOID", by.y = "cbsa", all.x = TRUE) %>%
    merge(dt_natl_clean, by = "index", all.x = TRUE) %>%
    merge(dt_mtg_clean, by = "index", all.x = TRUE) %>%
    merge(dt_saiz_clean, by = "GEOID", all.x = TRUE) %>%
    merge(dt_bsh_clean, by = "GEOID", all.x = TRUE)
  
  dt_out <- dt_out[, division_idx := paste0(census_division, "_", index)]
  
  dt_out <- dt_out[, `:=`(
    saiz_hp = saiz_elasticity * dlog_yoy_hp_natl,
    saiz_mtg_rate = saiz_elasticity * d_yoy_real_mtg_rate,
    bsh_hp = gamma01b_space_FMM * dlog_yoy_hp_natl,
    bsh_mtg_rate = gamma01b_space_FMM * d_yoy_real_mtg_rate
  )]
  
  setorder(dt_out, GEOID, index)
  
  cols_to_keep <- c("GEOID", "cz20", "st_first", "census_region", "census_division",
                    "index", "division_idx", "hp.target", "lu_ml_xgboost", 
                    "saiz_hp", "saiz_mtg_rate", 
                    "bsh_hp", "bsh_mtg_rate")
  
  dt_out <- dt_out %>% 
    select_by_ref(cols_to_keep) %>% 
    setcolorder(cols_to_keep)
  
  return(dt_out)
}


f_reg_sc_stage1_panel <- function(dt, start_idx, end_idx, hp_target_var,
                                  control_vars = NULL, geog_cluster_var,
                                  ds_label, geog_label,
                                  sc_vars = c("lu_ml_xgboost", "saiz_hp",
                                              "saiz_mtg_rate", "bsh_hp",
                                              "bsh_mtg_rate")) {
  
  box::use(
    data.table[...], magrittr[`%>%`], fixest[feols, xpd], broom[tidy, glance],
    lubridate[is.Date]
  )
  
  stopifnot(
    is.data.table(dt),
    (is.character(start_idx) | is.Date(start_idx) | is.numeric(start_idx)),
    is.Date(end_idx),
    is.character(hp_target_var) && length(hp_target_var) == 1,
    is.character(geog_cluster_var) && length(geog_cluster_var) == 1,
    is.character(ds_label) && length(ds_label) == 1,
    is.character(geog_label) && length(geog_label) == 1,
    geog_cluster_var %in% names(dt)
  )

  dt <- copy(dt)

  if (!is.null(control_vars)) {
    stopifnot(is.character(control_vars), all(control_vars %chin% names(dt)))
    dt <- na.omit(dt, cols = control_vars)
  }

  missing_sc_vars <- sc_vars[sc_vars %notin% names(dt)]
  if (length(missing_sc_vars) > 0) {
    warning(paste0("The following sc_vars are missing: ",
                   paste(missing_sc_vars, collapse = ", ")))
  }
  
  sc_vars <- sc_vars[sc_vars %chin% names(dt)]
  if (length(sc_vars) == 0) stop("No valid supply constraint variables found in data.")

  dt <- dt[, cluster_var := ..cluster_col, 
           env = list(..cluster_col = geog_cluster_var)]
  
  setnames(dt, hp_target_var, "hp.target")

  dt_nms <- c("GEOID", "index", "division_idx", "hp.target", sc_vars,
              control_vars, "cluster_var")
  
  stopifnot(all(dt_nms %chin% names(dt)))

  dt_reg_data <- dt[, ..dt_nms] %>%
    .[!is.na(hp.target)] %>%
    .[index >= start_idx & index <= end_idx] %>%
    .[, index_char := as.character(index)]

  if (nrow(dt_reg_data) == 0) {
    # Return a 1-row data.table with NAs so rbind works downstream
    empty_out <- data.table(
      ds_label = ds_label,
      geog_label = geog_label,
      est_type = "Differenced Panel (FE)", 
      hp_target_var = hp_target_var,
      start_idx = start_idx,
      end_idx = end_idx,
      geog_cluster_var = geog_cluster_var,
      num_possible_geoids = 0,
      sc_var = sc_vars,
      stage1_kp_f = NA_real_,
      stage1_partial_r2 = NA_real_,
      num_used_geoids = 0,
      se_type = NA_character_
    )
    return(empty_out)
  }

  multi_period_geoids <- dt_reg_data[, .(n_periods = uniqueN(index)),
                                     by = GEOID][n_periods > 1, GEOID]
  
  check_div <- dt_reg_data[GEOID %chin% multi_period_geoids,
                           .(n_divs = uniqueN(division_idx)), by = GEOID]
  
  bad_geoids <- check_div[n_divs == 1]
  
  if (nrow(bad_geoids) > 0) {
    bad_list <- paste(head(bad_geoids$GEOID, 5), collapse = ", ")
    stop(paste0("Error: `division_idx` is static (constant) for ", nrow(bad_geoids), 
                " multi-period GEOIDs (e.g., ", bad_list, "). ",
                "It must be a Division-Time interaction."))
  }

  dt_reg_data[, num_obs_per_geoid := .N, by = GEOID]
  max_obs <- max(dt_reg_data$num_obs_per_geoid)
  
  n_dropped <- uniqueN(dt_reg_data[num_obs_per_geoid < max_obs, GEOID])
  if (n_dropped > 0) {
    warning(paste0("Dropping ", n_dropped, " GEOIDs that do not have the full ", 
                   max_obs, " observations (forcing balanced panel)."))
  }
  
  dt_reg_data <- dt_reg_data[num_obs_per_geoid == max_obs]
  dt_reg_data[, num_obs_per_geoid := NULL]

  if (nrow(dt_reg_data) == 0)
    stop("All data was filtered out when balancing the panel.")

  run_first_stage <- function(curr_sc_var) {
    
    dt_curr <- dt_reg_data[!is.na(get(curr_sc_var))]
    
    if (is.null(control_vars)) {
      f_formula_main <- xpd(hp.target ~ ..sc | GEOID + division_idx, 
                            ..sc = curr_sc_var)
    } else {
      f_formula_main <- xpd(hp.target ~ ..sc + ..ctrls | GEOID + division_idx, 
                            ..sc = curr_sc_var, ..ctrls = control_vars)
    }
    
    mod <- try(feols(f_formula_main, data = dt_curr, 
                     cluster = as.formula(paste0("~", "cluster_var"))), 
               silent = TRUE)
    
    used_se_type <- "Clustered"

    if (inherits(mod, "try-error")) {
      warning(paste0("Clustered regression failed for ", curr_sc_var,
                     ". Retrying without clustering."))
      mod <- feols(f_formula_main, data = dt_curr)
      used_se_type <- "Unclustered"
    }
    
    stats <- summary(mod)$coeftable
    if (curr_sc_var %in% rownames(stats)) {
      t_stat <- stats[curr_sc_var, "t value"]
      kp_f_stat <- t_stat^2
    } else {
      stop(paste0("Coefficient ", curr_sc_var, " not found in model output."))
    }
    
    # ---------------------------------------------------------
    # (1) FWL for Partial R2 (Within-Transformation)
    # We regress Y and X on Controls + FEs, extract residuals,
    # and square their correlation. This isolates the variation
    # in Y explained by X *after* partialling out FEs and Controls.
    # ---------------------------------------------------------
    
    if (is.null(control_vars)) {
      f_lhs_resid <- xpd(hp.target ~ 1 | GEOID + division_idx)
      f_rhs_resid <- xpd(..sc ~ 1 | GEOID + division_idx, ..sc = curr_sc_var)
    } else {
      f_lhs_resid <- xpd(hp.target ~ ..ctrls | GEOID + division_idx, 
                         ..ctrls = control_vars)
      f_rhs_resid <- xpd(..sc ~ ..ctrls | GEOID + division_idx, 
                         ..sc = curr_sc_var, ..ctrls = control_vars)
    }
    
    resid_lhs <- resid(feols(f_lhs_resid, data = dt_curr))
    resid_rhs <- resid(feols(f_rhs_resid, data = dt_curr))
    
    partial_r2 <- cor(resid_lhs, resid_rhs)^2 

    return(list(
      mod = mod,
      kp_f_stat = kp_f_stat,
      partial_r2 = partial_r2,
      se_type = used_se_type,
      used_geoids = uniqueN(dt_curr$GEOID)
    ))
  }

  dt_out <- data.table(
    ds_label = ds_label,
    geog_label = geog_label,
    est_type = "Differenced Panel (FE)", 
    hp_target_var = hp_target_var,
    start_idx = start_idx,
    end_idx = end_idx,
    geog_cluster_var = geog_cluster_var,
    num_possible_geoids = uniqueN(dt_reg_data$GEOID),
    sc_var = sc_vars,
    control_vars = ifelse(is.null(control_vars), NA_character_, 
                          paste(control_vars, collapse = ", "))
  )

  results <- lapply(sc_vars, run_first_stage)
  
  dt_out[, stage1_kp_f := sapply(results, function(x) x$kp_f_stat)]
  dt_out[, stage1_partial_r2 := sapply(results, function(x) x$partial_r2)]
  dt_out[, num_used_geoids := sapply(results, function(x) x$used_geoids)]
  dt_out[, se_type := sapply(results, function(x) x$se_type)]
  dt_out[, mod := lapply(results, function(x) x$mod)]
  
  dt_out[, mod_broom_tidy := lapply(mod, function(m) as.data.table(tidy(m)))]
  dt_out[, mod_broom_glance := lapply(mod, function(m) as.data.table(glance(m)))]

  return(dt_out)
}
