## core/stroebelvavra_utils.R


#' Estimate Stroebel and Vavra (2019) Replication Models
est_stroebel_vavra_2019_models <- function(dt_lu_01_06, dt_lu_07_11, file_raw_01_06, file_raw_07_11, dt_bsh_data) {
  box::use(
    data.table[...],
    lfe[felm],
    magrittr[`%>%`],
    nanoparquet[read_parquet]
  )

  # -- 1. Data Prep -- #
  
  # BSH Data
  dt_bsh <- copy(dt_bsh_data) %>%
    setDT() %>%
    .[, .(cbsa2023 = as.character(GEOID_metro), bsh = gamma01b_space_FMM)]

  # Helper to prep period data
  prep_period_data <- function(file_raw, dt_lu) {
    # Clean LU Data: Rename keys to match SV data (cbsa -> cbsa2023)
    dt_lu_clean <- copy(dt_lu) %>%
      setnames("cbsa", "cbsa2023", skip_absent = TRUE) %>%
      setnames("lu_ml_xgboost", "lu_ml", skip_absent = TRUE) %>%
      .[, .(cbsa2023 = as.character(cbsa2023), lu_ml)]

    # Load Raw SV Data and Merge
    read_parquet(file_raw) %>%
      setDT() %>%
      .[, cbsa2023 := as.character(cbsa2023)] %>%
      merge(dt_bsh, by = "cbsa2023", all.x = TRUE) %>%
      merge(dt_lu_clean, by = "cbsa2023")
  }

  dt_01_06 <- prep_period_data(file_raw_01_06, dt_lu_01_06)
  dt_07_11 <- prep_period_data(file_raw_07_11, dt_lu_07_11)

  # -- 2. Estimation Helper Functions -- #

  # Function to run residual regression for partial R2 calculation
  f_reg_resid <- function(dt, rhs_var) {
    controls <- c("diffsharef", "diffsharen", "diffsharec", "diffu", "diffwage")
    
    # Ensure complete cases
    dt_subset <- dt[!is.na(d_index_sa) & !is.na(get(rhs_var))]
    for (ctl in controls) dt_subset <- dt_subset[!is.na(get(ctl))]

    # Residualize Y and X
    form_rhs <- as.formula(paste(rhs_var, "~", paste(controls, collapse = "+")))
    form_lhs <- as.formula(paste("d_index_sa ~", paste(controls, collapse = "+")))
    
    resid_rhs <- felm(form_rhs, data = dt_subset)$residuals
    resid_lhs <- felm(form_lhs, data = dt_subset)$residuals

    dt_res <- data.table(
      y_res = as.numeric(resid_lhs),
      x_res = as.numeric(resid_rhs),
      cbsa2023 = dt_subset$cbsa2023
    )
    
    # Run partial regression
    felm(y_res ~ 1 + x_res | 0 | 0 | cbsa2023, data = dt_res)
  }

  # Function to run the main regressions for a period
  run_period_models <- function(dt) {
    
    # Saiz
    m_saiz_1 <- felm(d_index_sa ~ elasticity | 0 | 0 | cbsa2023, data = dt)
    m_saiz_2 <- felm(d_index_sa ~ elasticity + diffsharef + diffsharen + diffsharec + diffu + diffwage | 0 | 0 | cbsa2023, data = dt)
    m_saiz_2_resid <- f_reg_resid(dt, "elasticity")

    # BSH
    m_bsh_1 <- felm(d_index_sa ~ bsh | 0 | 0 | cbsa2023, data = dt)
    m_bsh_2 <- felm(d_index_sa ~ bsh + diffsharef + diffsharen + diffsharec + diffu + diffwage | 0 | 0 | cbsa2023, data = dt)
    m_bsh_2_resid <- f_reg_resid(dt, "bsh")

    # LU-ML
    m_lu_1 <- felm(d_index_sa ~ lu_ml | 0 | 0 | cbsa2023, data = dt)
    m_lu_2 <- felm(d_index_sa ~ lu_ml + diffsharef + diffsharen + diffsharec + diffu + diffwage | 0 | 0 | cbsa2023, data = dt)
    m_lu_2_resid <- f_reg_resid(dt, "lu_ml")

    list(
      saiz_1 = m_saiz_1, saiz_2 = m_saiz_2, saiz_2_resid = m_saiz_2_resid,
      bsh_1 = m_bsh_1, bsh_2 = m_bsh_2, bsh_2_resid = m_bsh_2_resid,
      lu_1 = m_lu_1, lu_2 = m_lu_2, lu_2_resid = m_lu_2_resid
    )
  }

  # -- 3. Execute -- #
  res_01_06 <- run_period_models(dt_01_06)
  res_07_11 <- run_period_models(dt_07_11)

  return(list(p01_06 = res_01_06, p07_11 = res_07_11))
}

create_stroebel_vavra_2019_tex <- function(est_output, file_out) {
  box::use(
    stargazer[stargazer],
    starpolishr[...],
    magrittr[`%>%`],
    broom[tidy, glance],
    data.table[setDT],
    utils[capture.output]
  )

  # Helpers
  f_get_r2 <- function(mod) {
    glance(mod)$r.squared %>% sprintf("%0.2f", .)
  }
  
  f_get_f <- function(mod) {
    tidy(mod) %>% setDT() %>% 
      .[grepl("elasticity|bsh|lu_ml", term), statistic^2] %>% 
      sprintf("%0.2f", .)
  }

  generate_panel_table <- function(models, title) {
    mod_list <- list(
      models$saiz_1, models$saiz_2, 
      models$bsh_1, models$bsh_2, 
      models$lu_1, models$lu_2
    )
    
    # Capture noise
    capture.output(stargazer(mod_list, type = "text"))

    stargazer(
      mod_list, type = "latex",
      title = title,
      label = "tab:stroebel_vavra_2019_regs",
      keep = c("elasticity|bsh|lu_ml"),
      keep.stat = c("n", "rsq")
    ) %>%
      star_rhs_names(
        pattern = c("elasticity", "bsh", "lu\\\\_ml"),
        line1 = c("Saiz Elasticity", "Baum-Snow \\\\& Han", "LU-ML")
      ) %>%
      star_insert_row(
        string = c(
          sprintf("First Stage $F$-Stat & %s & %s & %s & %s & %s & %s \\\\", 
                  f_get_f(models$saiz_1), f_get_f(models$saiz_2),
                  f_get_f(models$bsh_1), f_get_f(models$bsh_2),
                  f_get_f(models$lu_1), f_get_f(models$lu_2)),
          sprintf("First Stage Partial $R^2$ & %s & %s & %s & %s & %s & %s \\\\", 
                  f_get_r2(models$saiz_1), f_get_r2(models$saiz_2_resid),
                  f_get_r2(models$bsh_1), f_get_r2(models$bsh_2_resid),
                  f_get_r2(models$lu_1), f_get_r2(models$lu_2_resid)),
          "Controls &  & \\checkmark &  & \\checkmark  &  & \\checkmark \\\\"
        ),
        insert.after = c(25, 25, 25)
      ) %>%
      .[!grepl("^R\\$\\^\\{2\\}\\$", x = .)]
  }

  star_01_06 <- generate_panel_table(est_output$p01_06, "\\textbf{First Stage House Price Regression (2001-2006)}")
  star_07_11 <- generate_panel_table(est_output$p07_11, "\\textbf{First Stage House Price Regression (2007-2011)}")

  star_all <- star_panel(
    star_01_06, star_07_11,
    panel.names = c("2001--06", "2007--11"),
    same.summary.stats = FALSE, 
    panel.label.fontface = "bold"
  ) %>%
    sub("\\\\textit\\{Dependent variable:\\}", "House Price Growth", x = .) %>%
    .[!grepl("d\\\\_index\\\\_sa", x = .)] %>%
    gsub("Observations", "Number of MSAs", x = .) %>%
    star_notes_tex(
      note.type = "caption",
      note = r"(Columns (1) and (2) replicate the first stage regression in Table A2, columns (1) and (5) of \citet{StroebelVavra2019} using the equation $\Delta \log(\textit{HP}_m) = \alpha + \beta \textit{Elasticity}_m + \gamma X_m + \varepsilon_m$. $\Delta \log(\textit{HP}_m)$ is the log difference in house prices for MSA $m$ between 2001--06 (panel A) or 2007--11 (panel B). \citet{StroebelVavra2019} use proprietary house prices from CoreLogic. This replication uses publicly available Freddie Mac House Price Indices. $\textit{Elasticity}_m$ is the Saiz elasticity proxy for MSA $m$. $X_m$ is a vector of controls for MSA $m$ and includes the change in the share of grocery retail employment, the change in the share of nontradable employment, the change in the share of construction employment, the change in the unemployment rate, and the change in the wage. See Table 1 in \citet{StroebelVavra2019} for more information on these controls. The number of MSAs in columns (1) and (2) is higher than in Table A2 of \citet{StroebelVavra2019} because their replication code includes an undisclosed outcome variable that limits the number of MSAs. Without this outcome variable, we could not determine which MSAs were used in their analysis. Here we report regressions results using all available MSAs in the \citet{StroebelVavra2019} data for each specification. Columns (3) and (4) use the elasticity proxy from \citet{BaumSnowHan2024} and columns (5) and (6) use our LU-ML instrument. Robust standard errors are in parentheses. One, two, or three asterisks represent statistical significance at the 10, 5, and 1 percent levels, respectively.)"
    )

  dir.create(dirname(file_out), showWarnings = FALSE, recursive = TRUE)
  star_write_and_compile_pdf(star_all, file = file_out)
  
  return(file_out)
}

extract_stroebel_vavra_2019_stats <- function(est_output) {
  box::use(
    data.table[...],
    broom[tidy, glance],
    magrittr[`%>%`]
  )

  f_get_f <- function(mod) {
    tidy(mod) %>% setDT() %>% .[grepl("elasticity|lu_ml|bsh", term), statistic^2]
  }

  dt_stats <- expand.grid(
    rhs_var = c("saiz", "bsh", "lu"),
    time_period = c("01_06", "07_11"),
    controls = c(TRUE, FALSE),
    stringsAsFactors = FALSE
  ) %>% setDT()

  # Populate Stats
  dt_stats[, `:=`(
    num_obs = NA_integer_, 
    stage1_fstat = NA_real_, 
    stage1_partial_r2 = NA_real_
  )]

  for (i in seq_len(nrow(dt_stats))) {
    p_label <- paste0("p", dt_stats$time_period[i])
    mod_suffix <- paste0(dt_stats$rhs_var[i], "_", ifelse(dt_stats$controls[i], "2", "1"))
    
    # Main Model
    mod <- est_output[[p_label]][[mod_suffix]]
    
    dt_stats[i, num_obs := glance(mod)$nobs]
    dt_stats[i, stage1_fstat := f_get_f(mod)]
    
    if (dt_stats$controls[i]) {
      # Use Residual Model for Partial R2
      resid_mod <- est_output[[p_label]][[paste0(mod_suffix, "_resid")]]
      dt_stats[i, stage1_partial_r2 := glance(resid_mod)$r.squared]
    } else {
      # Use Main Model R2 (since no controls)
      dt_stats[i, stage1_partial_r2 := glance(mod)$r.squared]
    }
  }

  return(dt_stats)
}
