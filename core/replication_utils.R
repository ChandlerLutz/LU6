## core/replication_utils.R

f_prep_ms_replication_data <- function(dt_ms, dt_bsh) {
  box::use(data.table[...], magrittr[`%>%`], CLmisc[select_by_ref])
  
  # Prepare BSH data (rename GEOID_cnty -> fips)
  dt_bsh_clean <- copy(dt_bsh) %>%
    setnames("GEOID_cnty", "fips", skip_absent = TRUE) %>%
    select_by_ref(c("fips", "gamma01b_space_FMM"))
  
  # dt_ms (from lu_ml targets) should already have Mian & Sufi data + LU data
  dt_out <- merge(dt_ms, dt_bsh_clean, by = "fips", all.x = TRUE)
  
  # --- Create state.fips for clustering ---
  dt_out[, state.fips := substr(fips, 1, 2)]
  
  # Filter NAs for control variables
  ind_control_vars <- paste0("C2D06share", 1:22)
  for (var in ind_control_vars) {
    dt_out <- dt_out[!is.na(get(var))]
  }
  
  return(dt_out)
}

f_run_ms_models <- function(dt) {
  box::use(
    lfe[felm], fixest[feols, xpd], data.table[...], magrittr[`%>%`],
    starpolishr[star_change_felm_rhs_names]
  )
  
  # -- Setup --
  ind_control_vars <- paste0("C2D06share", 1:22)
  
  # -- Helper: OLS --
  f_ols_reg <- function(lhs_var) {
    dt_tmp <- dt[!is.na(get(lhs_var)) & !is.na(house.net.worth)]
    
    f_ols <- xpd(..lhs ~ house.net.worth + ..ctrls,
                 ..lhs = lhs_var, ..ctrls = ind_control_vars)
    
    felm(f_ols, data = dt_tmp, weights = dt_tmp$housing.units)
  }
  
  # -- Helper: IV --
  f_iv_reg <- function(lhs_var, iv_vars, filter = NULL) {
    dt_tmp <- dt[!is.na(get(lhs_var)) & !is.na(house.net.worth)]
    for (var in iv_vars) dt_tmp <- dt_tmp[!is.na(get(var))]
    
    if (!is.null(filter)) {
      dt_tmp <- dt_tmp[eval(parse(text = filter))]
    }
    
    # 2SLS Model
    f_2sls <- xpd(..lhs ~ ..ctrls | 0 | (house.net.worth ~ ..ivs) | state.fips,
                  ..lhs = lhs_var, 
                  ..ctrls = ind_control_vars,
                  ..ivs = paste0(iv_vars, collapse = " + "))
    
    mod_2sls <- felm(f_2sls, data = dt_tmp, weights = dt_tmp$housing.units) %>%
      star_change_felm_rhs_names(old = "`house.net.worth(fit)`", new = "house.net.worth")
    
    # Stage 1 Partial R2 Logic
    vars_to_resid <- c(lhs_var, iv_vars, "house.net.worth")
    
    f_resid_formula <- xpd(..vars ~ ..ctrls,
                           ..vars = paste0("c(", paste0(vars_to_resid, collapse = ","), ")"),
                           ..ctrls = ind_control_vars)
    
    dt_resid <- feols(f_resid_formula, data = dt_tmp, weights = dt_tmp$housing.units) %>%
      lapply(\(m) m[["residuals"]]) %>%
      do.call("cbind", args = .) %>%
      as.data.table() %>%
      setnames(names(.), vars_to_resid) %>%
      cbind(dt_tmp[, .(fips, state.fips, housing.units)], .)
    
    f_stage1 <- xpd(house.net.worth ~ ..ivs, ..ivs = paste0(iv_vars, collapse = " + "))
    mod_stage1 <- feols(f_stage1, data = dt_resid, weights = dt_resid$housing.units)
    
    # Attach stats to model object
    mod_2sls$stage1.partial.r2 <- fixest::r2(mod_stage1)[["r2"]]
    mod_2sls$stage1.instruments <- iv_vars 
    
    return(mod_2sls)
  }
  
  # -- Run Models --
  m1 <- f_ols_reg("non.trade.emp.retail.rest")
  m2 <- f_ols_reg("non.trade.emp.geography")
  m3 <- f_iv_reg("non.trade.emp.retail.rest", "elasticity")
  m4 <- f_iv_reg("non.trade.emp.geography", "elasticity")
  m5 <- f_iv_reg("non.trade.emp.retail.rest", "gamma01b_space_FMM")
  m6 <- f_iv_reg("non.trade.emp.geography", "gamma01b_space_FMM")
  m7 <- f_iv_reg("non.trade.emp.retail.rest", "lu_ml_xgboost")
  m8 <- f_iv_reg("non.trade.emp.geography", "lu_ml_xgboost")
  
  # Store LHS explicitly
  mods <- list(m1, m2, m3, m4, m5, m6, m7, m8)
  lhs_vars <- c(
    "non.trade.emp.retail.rest", "non.trade.emp.geography",
    "non.trade.emp.retail.rest", "non.trade.emp.geography",
    "non.trade.emp.retail.rest", "non.trade.emp.geography",
    "non.trade.emp.retail.rest", "non.trade.emp.geography"
  )
  
  for(i in seq_along(mods)) {
    mods[[i]]$lhs_name <- lhs_vars[i]
  }

  return(mods)
}

# 1. New Function: Purely calculates stats (Renamed to regression_stats)
f_create_ms_regression_stats <- function(mods) {
  box::use(data.table[...], magrittr[`%>%`])
  
  # Helpers defined locally
  f_get_stage1_fstat_numeric <- function(model) {
    fstat <- model$stage1$iv1fstat$house.net.worth[["F"]]
    if (is.null(fstat)) return(NA_real_) else return(fstat)
  }
  f_get_stage2_se <- function(model) {
    se <- model$cse %>% .[grepl("house.net.worth\\(fit\\)", x = names(.))]
    if (is.null(se)) return(NA_real_) else return(se)
  }
  
  dt_stats <- data.table(mod = mods) %>%
    .[, lhs_var := sapply(mod, \(x) x$lhs_name)] %>%
    .[, num_obs := sapply(mod, \(x) x$N)] %>%
    .[, est_type := sapply(mod, \(x) fifelse(is.null(x$stage1), "OLS", "IV"))] %>%
    .[est_type == "IV", iv := sapply(mod, \(x) paste(x$stage1.instruments, collapse=","))] %>%
    .[, iv_row1_label := fcase(is.na(iv), "",
                               iv == "elasticity", "Saiz",
                               iv == "gamma01b_space_FMM", "Baum-Snow",
                               grepl("^lu_ml", iv), "LU-ML")] %>% 
    .[, iv_row2_label := fcase(is.na(iv), "",
                               iv == "elasticity", "Elasticity",
                               iv == "gamma01b_space_FMM", "\\& Han", 
                               grepl("^lu_ml", iv), "", 
                               default = "")] %>%
    .[, fstat := sapply(mod, f_get_stage1_fstat_numeric)] %>% 
    .[, stage1_partial_r2 := sapply(mod, \(x) if(is.null(x$stage1.partial.r2)) NA_real_ else x$stage1.partial.r2)] %>% 
    .[, stage2_se := sapply(mod, f_get_stage2_se)] %>%
    .[, mod := NULL]
  
  return(dt_stats)
}

# 2. Updated Function: Generates Table (Renamed to tex_table)
f_create_ms_tex_table <- function(mods, dt_regression_stats, output_tex) {
  box::use(
    data.table[...], magrittr[`%>%`], stargazer[stargazer],
    starpolishr[...], CLmisc[mkdir_p]
  )
  
  # -- Generate Stargazer --
  star_out <- capture.output(
    stargazer(
      mods, 
      title = r"(\textbf{Replication of \citet{MianSufi2014} -- Non-Tradable Employment and the Housing Net Worth})",
      label = "tab:ms_lu", 
      keep = c("house.net.worth"),
      keep.stat = "n",
      header = FALSE
    )
  ) 
  
  # -- Polish Table --
  dt_lhs_lkp <- data.table(lhs.var = star_get_lhs_names(star_out)) %>%
    .[lhs.var == "non.trade.emp.retail.rest", `:=`(line1 = "Rest. \\\\&", line2 = "Retail")] %>%
    .[lhs.var == "non.trade.emp.geography", `:=`(line1 = "Geog.", line2 = "Concen.")]
  
  star_out <- star_lhs_names(star_out, pattern = dt_lhs_lkp$lhs.var,
                             line1 = dt_lhs_lkp$line1,
                             line2 = dt_lhs_lkp$line2) %>% 
    star_rhs_names(pattern = "house.net.worth",
                   line1 = "$\\\\Delta$ Housing Net Worth, 2006-09") %>% 
    sub("Dependent variable:", "Non-Tradable Emp. Growth, 2007-09", x = .)

  # -- Insert Stats (using dt_regression_stats argument) --
  f_fmt <- function(x) if (is.na(x)) "" else sprintf("%.02f", x)
  
  stage1_fstat <- paste0(sapply(dt_regression_stats$fstat, f_fmt), collapse = " & ") %>%
    paste0("First Stage $F$-Stat & ", ., " \\\\")
  
  stage1_pr2 <- paste0(sapply(dt_regression_stats$stage1_partial_r2, f_fmt), collapse = " & ") %>%
    paste0("First Stage Partial $R^2$ & ",., " \\\\")
  
  obs_row <- grep("^Observations", star_out)[1]
  star_ncol <- star_ncol(star_out)
  
  star_out <- star_insert_row(
    star_out,
    string = c(sprintf("\\cline{2-%s} \\\\[-2.0ex]", star_ncol),
               stage1_fstat, stage1_pr2, "\\\\[-1.83ex]"),
    insert.after = obs_row - 2
  )

  specification <- paste0("Specification & ", paste0(dt_regression_stats$est_type, collapse = " & "), " \\\\")
  iv_row1 <- paste0("Instrument & ", paste0(dt_regression_stats$iv_row1_label, collapse = " & "), " \\\\")
  iv_row2 <- paste0(" & ", paste0(dt_regression_stats$iv_row2_label, collapse = " & "), " \\\\")

  obs_row <- grep("^Observations", star_out)[1]
  star_out <- star_insert_row(
    star_out, 
    string = c(specification, iv_row1, iv_row2, " \\\\[-2.0ex]"),
    insert.after = obs_row - 1
  ) %>%
    sub("Observations", "Number of Counties", x = .) %>%
    star_notes_tex(
      note.type = "caption",
      note = r"(Columns 1 to 4 replicate \citet{MianSufi2014} using the equation $\Delta \log E_i^{NT} = \alpha + \eta \cdot \Delta \textit{HNW}_{i} + \varepsilon_i$. $\Delta \log E_i^{NT}$ is the log change in non-tradable employment for county $i$ and $\textit{HNW}_i$ is the change in housing net worth for county $i$. Mian and Sufi proxy non-tradable employment via the restaurant and retail sector (\textit{Rest. and Retail}) or through industries that have low geographic concentration (\textit{Geog. Concen.}). Columns (5) to (6) use the elasticity proxy from \citet{BaumSnowHan2024} as an instrument, while columns (7) and (8) employ the LU-ML IV. Controls include 23 two-digit 2006 employment shares. Robust standard errors are clustered by state. One, two, or three asterisks represent statistical significance at the 10, 5, and 1 percent levels, respectively.)"
    ) %>%
    star_sidewaystable()

  mkdir_p(dirname(output_tex))
  star_write_and_compile_pdf(star_out, file = output_tex)
  
  return(output_tex)
}
