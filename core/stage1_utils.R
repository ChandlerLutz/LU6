## core/stage1_utils.R

f_reg_sc_stage1_panel <- function(dt, start_idx, end_idx, hp_target_var,
                                  control_vars = NULL, geog_cluster_var,
                                  ds_label, geog_label,
                                  sc_vars = c("lu_ml_xgboost", "saiz_hp", "saiz_mtg_rate", "bsh_hp", "bsh_mtg_rate")) {
  
  # --- 0. Imports ---
  box::use(
    data.table[...],
    lfe[felm],
    broom[tidy, glance],
    magrittr[`%>%`],
    CLmisc[reduce_felm_object_size],
    stats[resid, lm, coef, as.formula, na.omit, setNames]
  )
  
  `%notin%` <- Negate(`%in%`)

  # --- 1. Input Validation ---
  stopifnot(
    is.data.table(dt),
    (is.character(start_idx) | is.Date(start_idx) | is.numeric(start_idx)),
    (is.character(end_idx) | is.Date(end_idx) | is.numeric(end_idx)),
    is.character(hp_target_var) && length(hp_target_var) == 1,
    is.character(geog_cluster_var) && length(geog_cluster_var) == 1,
    is.character(ds_label) && length(ds_label) == 1,
    is.character(geog_label) && length(geog_label) == 1
  )

  dt <- copy(dt)

  # Check Controls
  if (!is.null(control_vars)) {
    stopifnot(is.character(control_vars), all(control_vars %chin% names(dt)))
    dt <- na.omit(dt, cols = control_vars)
  }

  # Check SC Vars
  missing_sc_vars <- sc_vars[sc_vars %notin% names(dt)]
  if (length(missing_sc_vars) > 0) {
    warning(paste0("The following sc_vars are missing: ", paste(missing_sc_vars, collapse = ", ")))
  }
  
  sc_vars <- sc_vars[sc_vars %chin% names(dt)]
  if (length(sc_vars) == 0) stop("No valid supply constraint variables found in data.")

  # --- 2. Data Prep ---
  
  dt[, cluster_var := get(geog_cluster_var)]
  setnames(dt, hp_target_var, "hp.target")

  dt_nms <- c("GEOID", "index", "division_idx", "hp.target", sc_vars,
              control_vars, "cluster_var")
  
  stopifnot(all(dt_nms %chin% names(dt)))

  # Filter Data (Time and NAs)
  dt_reg_data <- dt[, ..dt_nms] %>%
    .[!is.na(hp.target)] %>%
    .[index >= start_idx & index <= end_idx] %>%
    .[, index_char := as.character(index)]

  # --- 3. Critical Checks & Balancing ---

  # CHECK A: Division x Time Validity (Stricter Logic)
  # Identify GEOIDs that exist for >1 period
  multi_period_geoids <- dt_reg_data[, .(n_periods = uniqueN(index)), by = GEOID][n_periods > 1, GEOID]
  
  # For these GEOIDs, count unique division_idx values
  check_div <- dt_reg_data[GEOID %chin% multi_period_geoids, .(n_divs = uniqueN(division_idx)), by = GEOID]
  
  # Fail if ANY multi-period GEOID has a static division_idx (n_divs == 1)
  bad_geoids <- check_div[n_divs == 1]
  
  if (nrow(bad_geoids) > 0) {
    # Optional: print first few bad GEOIDs for debugging
    bad_list <- paste(head(bad_geoids$GEOID, 5), collapse = ", ")
    stop(paste0("Error: `division_idx` is static (constant) for ", nrow(bad_geoids), 
                " multi-period GEOIDs (e.g., ", bad_list, "). ",
                "It must be a Division-Time interaction (e.g., paste0(div, '_', index))."))
  }

  # CHECK B: Force Strictly Balanced Panel
  dt_reg_data[, num_obs_per_geoid := .N, by = GEOID]
  max_obs <- max(dt_reg_data$num_obs_per_geoid)
  
  n_dropped <- uniqueN(dt_reg_data[num_obs_per_geoid < max_obs, GEOID])
  if (n_dropped > 0) {
    warning(paste0("Dropping ", n_dropped, " GEOIDs that do not have the full ", 
                   max_obs, " observations (forcing balanced panel)."))
  }
  
  dt_reg_data <- dt_reg_data[num_obs_per_geoid == max_obs]
  dt_reg_data[, num_obs_per_geoid := NULL]

  if (nrow(dt_reg_data) == 0) stop("All data was filtered out when balancing the panel.")

  # --- 4. Regression Helper ---
  
  run_first_stage <- function(curr_sc_var) {
    
    dt_curr <- dt_reg_data[!is.na(get(curr_sc_var))]
    rhs_parts <- c(curr_sc_var, control_vars)
    
    # 1. Main Regression (With Try/Catch Fallback)
    
    # Attempt A: Standard Clustered SEs
    f_formula_clustered <- as.formula(
      paste0("hp.target ~ ", paste(rhs_parts, collapse = " + "), 
             " | GEOID + division_idx | 0 | cluster_var")
    )
    
    mod <- try(felm(f_formula_clustered, data = dt_curr), silent = TRUE)
    
    # Attempt B: Fallback (No Clustering) if Attempt A failed
    if (inherits(mod, "try-error")) {
      warning(paste0("Clustered regression failed for ", curr_sc_var, ". Retrying without clustering."))
      
      f_formula_nocluster <- as.formula(
        paste0("hp.target ~ ", paste(rhs_parts, collapse = " + "), 
               " | GEOID + division_idx | 0 | 0")
      )
      
      mod <- felm(f_formula_nocluster, data = dt_curr)
    }
    
    # Extract F-stat (Inference)
    tidy_mod <- as.data.table(tidy(mod))
    stat_row <- tidy_mod[term == curr_sc_var]
    
    if (nrow(stat_row) == 0) stop(paste0("Error extracting term '", curr_sc_var, "'."))
    
    f_stat <- stat_row$statistic^2
    main_coef <- stat_row$estimate
    
    # 2. FWL Regression (for correct Partial R2)
    
    if (is.null(control_vars)) {
      f_lhs_resid <- as.formula("hp.target ~ 1 | GEOID + division_idx")
      f_rhs_resid <- as.formula(paste0(curr_sc_var, " ~ 1 | GEOID + division_idx"))
    } else {
      ctrl_form <- paste(control_vars, collapse = " + ")
      f_lhs_resid <- as.formula(paste0("hp.target ~ ", ctrl_form, " | GEOID + division_idx"))
      f_rhs_resid <- as.formula(paste0(curr_sc_var, " ~ ", ctrl_form, " | GEOID + division_idx"))
    }
    
    mod_lhs <- felm(f_lhs_resid, data = dt_curr)
    mod_rhs <- felm(f_rhs_resid, data = dt_curr)
    
    resid_lhs <- resid(mod_lhs)
    resid_rhs <- resid(mod_rhs)
    
    # Partial R2 from residual regression through origin
    mod_fwl <- lm(resid_lhs ~ 0 + resid_rhs)
    partial_r2 <- summary(mod_fwl)$r.squared
    fwl_coef <- coef(mod_fwl)[["resid_rhs"]]

    # Cross-Check: Coefficient stability
    # The FWL coefficient should mathematically match the main model coefficient
    if (abs(fwl_coef - main_coef) > 1e-5) {
      warning(paste0("Mismatch in coefficients for ", curr_sc_var, 
                     ": Main = ", round(main_coef, 5), 
                     ", FWL = ", round(fwl_coef, 5)))
    }

    # Reduce object size
    mod <- reduce_felm_object_size(mod)
    
    return(list(
      mod = mod,
      f_stat = f_stat,
      partial_r2 = partial_r2,
      used_geoids = uniqueN(dt_curr$GEOID)
    ))
  }

  # --- 5. Execution ---
  
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
    control_vars = ifelse(is.null(control_vars), NA_character_, paste(control_vars, collapse = ", "))
  )

  results <- lapply(sc_vars, run_first_stage)
  
  dt_out[, stage1_f := sapply(results, function(x) x$f_stat)]
  dt_out[, stage1_partial_r2 := sapply(results, function(x) x$partial_r2)]
  dt_out[, num_used_geoids := sapply(results, function(x) x$used_geoids)]
  dt_out[, mod := lapply(results, function(x) x$mod)]
  
  dt_out[, mod_broom_tidy := lapply(mod, function(m) as.data.table(tidy(m)))]
  dt_out[, mod_broom_glance := lapply(mod, function(m) as.data.table(glance(m)))]

  return(dt_out)
}
