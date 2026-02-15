## core/chaneyetal_utils.R

est_chaneyetal_sc_lu_ml_models <- function(dt_chaneyetal_lu_ml, file_raw_chaney, dt_bsh_data, dt_real_mtg_rate) {
  box::use(
    data.table[...],
    lfe[felm],
    magrittr[`%>%`],
    nanoparquet[read_parquet]
  )

  # -- 1. Data Preparation -- #

  # A. Load Raw Chaney Data (Base)
  # This provides 'saiz_rate', 'msa' names, and the structure for 'id' generation
  dt_raw <- read_parquet(file_raw_chaney) %>% 
    setDT() %>%
    .[, id := as.character(.GRP), by = .(msacode, msacode_orig, msa)]
  
  # Remove existing real_mtg_rate from raw if present, to avoid duplicates/confusion with FRED data
  if ("real_mtg_rate" %in% names(dt_raw)) dt_raw[, real_mtg_rate := NULL]

  # B. Prepare ML Predictions
  dt_ml <- as.data.table(dt_chaneyetal_lu_ml) %>%
    .[, .(id, year, lu_ml = lu_ml_xgboost)] # Keep only keys and prediction

  # C. Prepare BSH Data
  dt_bsh_clean <- as.data.table(dt_bsh_data) %>%
    .[, .(msacode = as.integer(GEOID_metro), gamma01b_space_FMM)]

  # D. Prepare Real Mortgage Rate (Annualize)
  dt_rate_annual <- as.data.table(dt_real_mtg_rate) %>%
    .[, .(real_mtg_rate = mean(real_mtg_rate, na.rm = TRUE)), by = .(year)]

  # -- 2. Merge and Construct Variables -- #
  
  DT <- dt_raw %>%
    # Merge ML predictions (by id/year to match user logic)
    merge(dt_ml, by = c("id", "year"), all.x = TRUE) %>%
    # Merge BSH Elasticity
    .[, msacode := as.integer(msacode)] %>%
    merge(dt_bsh_clean, by = "msacode", all.x = TRUE) %>%
    # Merge FRED Real Mortgage Rate
    merge(dt_rate_annual, by = "year", all.x = TRUE) %>%
    # Construct BSH Instrument
    .[, bsh_rate := gamma01b_space_FMM * real_mtg_rate] %>%
    # Filter Sample
    .[year >= 1993] %>%
    # Create Character FEs
    .[, `:=`(msa_char = as.character(msa), year_char = as.character(year))]

  # -- 3. Estimate Models -- #
  
  # Model 1: Saiz (Uses 'saiz_rate' from raw file)
  mod_saiz <- felm(index_normalized ~ saiz_rate | msa_char + year_char | 0 | msa_char,
                   data = DT)
  
  # Model 2: BSH (Uses constructed 'bsh_rate')
  mod_bsh <- felm(index_normalized ~ bsh_rate | msa_char + year_char | 0 | msa_char,
                  data = DT)
  
  # Model 3: LU-ML (Uses merged 'lu_ml')
  mod_lu_ml <- felm(index_normalized ~ lu_ml | msa_char + year_char | 0 | msa_char,
                    data = DT)

  return(list(
    mod_saiz = mod_saiz,
    mod_bsh = mod_bsh,
    mod_lu_ml = mod_lu_ml,
    data = DT
  ))
}

create_chaneyetal_sc_lu_ml_tex <- function(est_output, file_out) {
  box::use(
    data.table[...],
    lfe[felm],
    stargazer[stargazer],
    starpolishr[...],
    magrittr[`%>%`],
    broom[tidy, glance],
    utils[capture.output]
  )

  DT <- est_output$data
  mod_saiz <- est_output$mod_saiz
  mod_bsh <- est_output$mod_bsh
  mod_lu_ml <- est_output$mod_lu_ml

  # -- Helper: Partial Regression for Stats -- #
  f_reg_resid <- function(rhs) {
    dt_subset <- DT[!is.na(index_normalized) & !is.na(get(rhs))]
    
    rhs_resid <- felm(as.formula(paste(rhs, "~ 1 | msa_char + year_char")), data = dt_subset)$residuals
    lhs_resid <- felm(index_normalized ~ 1 | msa_char + year_char, data = dt_subset)$residuals
    
    dt_resid <- data.table(
      y_resid = as.numeric(lhs_resid),
      x_resid = as.numeric(rhs_resid),
      msa_char = dt_subset$msa_char
    )
    
    felm(y_resid ~ 0 + x_resid | 0 | 0 | msa_char, data = dt_resid)
  }

  # -- Helper Stats Functions -- #
  f_get_stage1_f <- function(mod) {
    tidy(mod) %>% setDT() %>% .[, statistic^2] %>% sprintf("%0.2f", .)
  }

  f_get_stage1_partial_r2 <- function(var) {
    f_reg_resid(var) %>% glance() %>% setDT() %>% .[, r.squared] %>% sprintf("%0.2f", .)
  }

  f_get_num_msas <- function(var) {
    DT[!is.na(index_normalized) & !is.na(get(var)), uniqueN(msa)]
  }

  # -- Generate Table -- #
  capture.output(stargazer(mod_saiz, mod_bsh, mod_lu_ml, type = "text"))

  star <- stargazer(
    mod_saiz, mod_bsh, mod_lu_ml,
    type = "latex",
    title = "\\textbf{First Stage House Price Regression as in \\citet{ChaneyEtal2012}}",
    label = "tab:chaney_etal_2012_regs",
    keep.stat = c("n", "rsq")
  ) %>%
    sub("index\\\\_normalized", "MSA HP Residential Prices", x = .) %>%
    star_rhs_names(
      pattern = c("saiz\\\\_rate", "bsh\\\\_rate", "lu\\\\_ml"),
      line1 = c("Saiz Elasticity $\\\\times$", "Baum-Snow \\\\& Han $\\\\times$", "LU-ML"),
      line2 = c("Natl Mortgage Rate", "Natl Mortgage Rate", "")
    ) %>%
    star_insert_row(
      string = c(
        sprintf("First Stage $F$-Stat & %s & %s & %s \\\\",
                f_get_stage1_f(mod_saiz), f_get_stage1_f(mod_bsh), f_get_stage1_f(mod_lu_ml)),
        sprintf("First Stage Partial $R^{2}$ & %s & %s & %s \\\\",
                f_get_stage1_partial_r2("saiz_rate"), f_get_stage1_partial_r2("bsh_rate"), f_get_stage1_partial_r2("lu_ml")),
        "\\\\[-2.0ex] \\hline \\\\[-2.0ex]",
        sprintf("Number of MSAs & %s & %s & %s \\\\",
                f_get_num_msas("saiz_rate"), f_get_num_msas("bsh_rate"), f_get_num_msas("lu_ml")),
        "\\\\[-2.0ex] \\hline \\\\[-2.0ex]",
        "MSA Fixed Effects & \\checkmark & \\checkmark & \\checkmark \\\\",
        "Time Fixed Effects & \\checkmark & \\checkmark & \\checkmark \\\\"
      ),
      insert.after = c(24, 24, 24, 24, 26)
    ) %>%
    .[!grepl("^R\\$\\^\\{2\\}\\$", x = .)] %>%
    star_notes_tex(
      note.type = "caption",
      note = "Column (1) replicates the first stage regression in Table 3, column (1) of \\citet{ChaneyEtal2012} that uses Saiz Elasticity as an instrument for house prices using their equation (2): $P_t^l = \\alpha^l + \\delta_t + \\gamma \\cdot \\textit{Elasticity}^l \\times \\textit{IR}_t + u_t^l$. $P_t^l$ is the normalized residential house price index (in levels) for MSA $l$ in year $t$, $\\alpha^l$ and  $\\delta_t$ are MSA and time fixed effects, $\\textit{Elasticity}^l \\times \\textit{IR}_t$ is the Saiz Elasticity proxy for each MSA multiplied by the national real mortgage rate, and $u_t^l$ is the error term. Column (2) uses our LU-ML instrument for 2007 MSAs instead of $\\textit{Elasticity}^l \\times \\textit{IR}_t$. The sample period ranges from 1993 to 2007. Robust standard errors clustered at the MSA level are in parentheses. One, two, or three asterisks represent statistical significance at the 10, 5, and 1 percent levels, respectively."
    )

  dir.create(dirname(file_out), showWarnings = FALSE, recursive = TRUE)
  star_write_and_compile_pdf(star, file = file_out)
  
  return(file_out)
}

extract_chaneyetal_sc_lu_ml_reg_stats <- function(est_output) {
  box::use(
    data.table[...],
    lfe[felm],
    magrittr[`%>%`],
    broom[tidy, glance]
  )

  DT <- est_output$data
  mod_saiz <- est_output$mod_saiz
  mod_bsh <- est_output$mod_bsh
  mod_lu_ml <- est_output$mod_lu_ml

  # -- Helper: Partial Regression for Stats -- #
  f_reg_resid <- function(rhs) {
    dt_subset <- DT[!is.na(index_normalized) & !is.na(get(rhs))]
    
    rhs_resid <- felm(as.formula(paste(rhs, "~ 1 | msa_char + year_char")), data = dt_subset)$residuals
    lhs_resid <- felm(index_normalized ~ 1 | msa_char + year_char, data = dt_subset)$residuals
    
    dt_resid <- data.table(
      y_resid = as.numeric(lhs_resid),
      x_resid = as.numeric(rhs_resid),
      msa_char = dt_subset$msa_char
    )
    
    felm(y_resid ~ 0 + x_resid | 0 | 0 | msa_char, data = dt_resid)
  }

  f_get_stage1_f_numeric <- function(mod) {
    tidy(mod) %>% setDT() %>% .[, statistic^2]
  }

  f_get_stage1_partial_r2_numeric <- function(var) {
    f_reg_resid(var) %>% glance() %>% setDT() %>% .[, r.squared]
  }

  f_get_num_msas <- function(var) {
    DT[!is.na(index_normalized) & !is.na(get(var)), uniqueN(msa)]
  }

  dt_model_stats <- data.table(
    rhs_var = c("saiz_rate", "bsh_rate", "lu_ml"),
    num_obs = c(glance(mod_saiz)$nobs, glance(mod_bsh)$nobs, glance(mod_lu_ml)$nobs),
    num_msas = c(f_get_num_msas("saiz_rate"), f_get_num_msas("bsh_rate"), f_get_num_msas("lu_ml")),
    stage1_fstat = c(
      f_get_stage1_f_numeric(mod_saiz), 
      f_get_stage1_f_numeric(mod_bsh), 
      f_get_stage1_f_numeric(mod_lu_ml)
    ),
    stage1_partial_r2 = c(
      f_get_stage1_partial_r2_numeric("saiz_rate"),
      f_get_stage1_partial_r2_numeric("bsh_rate"),
      f_get_stage1_partial_r2_numeric("lu_ml")
    )
  )

  return(dt_model_stats)
}
