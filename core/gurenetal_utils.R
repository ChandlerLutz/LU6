f_add_stage1_r2_to_felm_model <- function(felm.model) {
  stage1.summary <- summary(felm.model$stage1)
  felm.model$stage1.r2 <- stage1.summary$r2
  return(felm.model)
}

f_prep_guren_merged_data <- function(dt_ml, file_guren_raw, dt_bsh) {
  
  box::use(
    data.table[...], magrittr[`%>%`], 
    fst[read_fst], CLmisc[select_by_ref]
  )

  # 1. Prepare ML Predictions (from your existing target)
  # dt_ml usually has cbsa as character and index as Date
  DT.ml <- copy(dt_ml) %>%
    setnames("index", "date") %>%
    .[, cbsa := as.numeric(cbsa)] %>% 
    select_by_ref(c("cbsa", "date", "lu_ml_xgboost")) %>%
    setnames("lu_ml_xgboost", "z_lu_ml")

  # 2. Prepare BSH Data
  DT.bsh <- copy(dt_bsh) %>%
    setnames("GEOID_metro", "cbsa") %>%
    .[, cbsa := as.numeric(cbsa)] %>%
    select_by_ref(c("cbsa", "gamma01b_space_FMM"))

  # 3. Load FULL Guren Data (to recover controls/FEs)
  DT <- read_fst(file_guren_raw, as.data.table = TRUE) %>%
    .[, region.char := as.character(region)] %>%
    .[, date.region.char := paste0(date.char, "_", region.char)]

  # 4. Merge Everything
  DT <- DT %>%
    merge(DT.ml, by = c("cbsa", "date"), all.x = TRUE) %>%
    merge(DT.bsh, by = "cbsa", all.x = TRUE) %>%
    .[, z_saiz := elasticity * lhpi_usa_a] %>%
    .[, z_saiz_unaval := unaval * lhpi_usa_a] %>%
    .[, z_bsh := gamma01b_space_FMM * lhpi_usa_a]

  # 5. Variable Selection
  vars.to.keep <- c(
    "cbsa", "date",
    "lq_retail_a_nobad_pc", "lhpi_a", 
    "predict_control", "pc_additional_1", "pc_additional_2",
    "date.char", "cbsa.char", "date.region.char", "region.char", "division", 
    names(DT) %>% .[grepl("^dateX", x = .)],
    names(DT) %>% .[grepl("^z", x = .)],
    "bl.ntile" 
  )
  
  DT <- DT %>%
    select_by_ref(vars.to.keep) %>%
    .[!is.na(z_lu_ml)] # Keep only rows where we have ML predictions

  return(DT)
}

f_estimate_guren_regressions <- function(DT) {
  
  box::use(
    data.table[...], magrittr[`%>%`], fixest[feols, xpd], lfe[felm],
    CLmisc[reduce_felm_object_size]
  )

  # Define Regression Options
  DT.reg.options <- expand.grid(
    startyr = c(1978, 1990, 2000),
    ind.controls = c(TRUE), 
    prediction.controls = c(TRUE),
    buildable.land.controls = c(FALSE),
    fixed.effects = c("date.region"),
    stringsAsFactors = FALSE
  ) %>% setDT() %>%
    .[, id := seq_len(.N)] %>%
    setcolorder(c("id"))

  # Worker Function
  f_run_regressions <- function(reg.options.id) {
    
    DT.opts <- DT.reg.options[id == c(reg.options.id)]
    startyr <- DT.opts$startyr

    DT.tmp <- DT[year(date) >= startyr]

    # Drop old industry controls
    ind.share.vars.to.drop <- data.table(
      ind.share.var = (names(DT.tmp) %>% .[grepl("dateX.share", x = .)])
    ) %>%
      .[, year := gsub(".*([0-9]{4})_[0-9]{2}_[0-9]{2}$", "\\1", x = ind.share.var)] %>%
      .[as.numeric(year) < startyr] %>%
      .[, ind.share.var]

    if (length(ind.share.vars.to.drop) > 0) {
      DT.tmp[, c(ind.share.vars.to.drop) := NULL]
    }

    if (DT.opts$buildable.land.controls == TRUE)
      DT.tmp <- DT.tmp[!is.na(bl.ntile)]

    DT.tmp.saiz <- DT.tmp[!is.na(z_saiz)]
    DT.tmp.bsh <- DT.tmp[!is.na(z_bsh)]

    # Controls
    controls <- c("1")
    DT.names <- names(DT.tmp)
    if (DT.opts$ind.controls == TRUE)
      controls <- c(controls, DT.names %>% .[grepl("dateX.share", x = .)])
    if (DT.opts$prediction.controls == TRUE)
      controls <- c(controls, DT.names %>% .[grepl("predict_control|pc_additional",x = .)])

    # FEs
    fe <- "date.char"
    if (DT.opts$fixed.effects == "date.region") {
      fe <- paste0(fe, " ^ region.char")
    } else if (DT.opts$fixed.effects == "date.division") {
      fe <- paste0(fe, " ^ division")
    }
    if (DT.opts$buildable.land.controls == TRUE)
      fe <- paste0(fe, " + bl.ntile")

    # Residualization Vars
    vars.to.residualize = DT.names %>%
      .[grepl("^z", x = .)] %>%
      .[!(. %chin% c("z_saiz", "z_saiz_unaval"))] %>%
      c("lq_retail_a_nobad_pc", "lhpi_a", .)
    
    vars.to.residualize.saiz <- c("lq_retail_a_nobad_pc", "lhpi_a", "z_saiz",
                                  "z_saiz_unaval")
    vars.to.residualize.bsh <- c("lq_retail_a_nobad_pc", "lhpi_a", "z_bsh")

    f_residualize <- function(lhs.vars, DT_in) {
      formula <- xpd(..lhs.vars ~ ..controls | ..fe,
                     ..lhs.vars = paste0("c(", paste0(lhs.vars, collapse = ", "), ")"), 
                     ..controls = controls,
                     ..fe = fe
      )
      DT.out <- feols(formula, data = DT_in) %>%
        lapply(\(m) m[["residuals"]]) %>%
        do.call("cbind", args = .) %>%
        as.data.table %>% setnames(names(.), lhs.vars) %>%
        cbind(DT_in[, .(cbsa.char, date.char)], .)
    }

    DT.residualized <- f_residualize(lhs.vars = vars.to.residualize, DT.tmp)
    DT.residualized.saiz <- f_residualize(lhs.vars = vars.to.residualize.saiz, DT.tmp.saiz)
    DT.residualized.bsh <- f_residualize(lhs.vars = vars.to.residualize.bsh, DT.tmp.bsh)

    # Model Functions
    f_reg_ols <- function(DT_reg) {
      f.felm <- xpd(
        lq_retail_a_nobad_pc ~ lhpi_a | 0 | 0 | date.char + cbsa.char
      )
      mod.felm.ols <- felm(f.felm, data = DT_reg) %>%
        reduce_felm_object_size
      return(mod.felm.ols)
    }

    f_reg <- function(ivs, DT_reg) {
      f.felm <- xpd(
        lq_retail_a_nobad_pc ~ 1 | 0 | (lhpi_a ~ ..ivs) | date.char + cbsa.char,
        ..ivs = ivs
      )
      f.feols <- xpd(
        lq_retail_a_nobad_pc ~ 1 | 0 | lhpi_a ~ ..ivs,
        ..ivs = ivs
      )
      mod.felm <- felm(f.felm, data = DT_reg) %>%
        f_add_stage1_r2_to_felm_model(.) %>% 
        reduce_felm_object_size
      
      mod.feols <- feols(f.feols, data = DT_reg, lean = TRUE)
      mod.felm$iv_wh <- mod.feols$iv_wh
      if ("iv_sargan" %chin% names(mod.feols))
        mod.felm$iv_sargan <- mod.feols$iv_sargan

      return(mod.felm)
    }

    # Run Models
    mod.ols <- f_reg_ols(DT.residualized)
    mod.sensitivity <- f_reg(ivs = "z", DT.residualized)
    mod.saiz <- f_reg(ivs = "z_saiz", DT.residualized.saiz)
    mod.bsh <- f_reg(ivs = "z_bsh", DT.residualized.bsh)
    mod.best.ml.lu <- f_reg(ivs = c("z_lu_ml"), DT.residualized)
    mod.sensitivity.best.ml.lu <- f_reg(ivs = c("z_lu_ml", "z"), DT.residualized)

    # Return Result
    data.table(
      reg.options.id = c(reg.options.id),
      mod.ols = list(mod.ols),
      mod.sensitivity = list(mod.sensitivity),
      mod.saiz = list(mod.saiz),
      mod.bsh = list(mod.bsh),
      mod.best.ml.lu = list(mod.best.ml.lu),
      mod.sensitivity.best.ml.lu = list(mod.sensitivity.best.ml.lu)
    )
  }

  # Run all options
  DT.reg.output <- lapply(DT.reg.options$id, f_run_regressions) %>%
    rbindlist()

  # Merge back options info
  DT.reg.output <- merge(DT.reg.options, DT.reg.output,
                         by.x = "id", by.y = "reg.options.id")

  return(DT.reg.output)
}

f_write_guren_tex_table <- function(DT.reg.output, output_tex) {
  
  box::use(
    data.table[...], magrittr[`%>%`], stargazer[...], 
    starpolishr[star_change_felm_rhs_names, star_rhs_names, star_insert_row, 
                star_panel, star_notes_tex, star_write_and_compile_pdf]
  )
  
  # --- 1. Construct Lookup Table (DT.models.lkp) ---
  DT.models.lkp <- data.table(
    fixed.effects = "date.region",
    buildable.land.controls = FALSE,
    model.type = c("mod.ols", "mod.saiz", "mod.sensitivity", "mod.bsh", 
                   "mod.best.ml.lu", "mod.sensitivity.best.ml.lu")
  ) %>%
    .[, mod.order := seq_len(.N)] %>%
    .[, date.fe := fifelse(fixed.effects == "date", "\\checkmark", " ")] %>%
    .[, date.region.fe := fifelse(fixed.effects == "date.region", "\\checkmark", " ")] %>%
    .[, bl.fe := fifelse(buildable.land.controls == TRUE, "\\checkmark", " ")] %>%
    .[, num.cbsas := fcase(grepl("saiz", model.type), 270,
                           buildable.land.controls == TRUE, 373,
                           grepl("bsh", model.type), 311,
                           default = 376)] %>%
    .[, ivs := fcase(
      model.type == "mod.saiz", list(c("Saiz", "", "")),
      model.type == "mod.bsh", list(c("Baum-Snow", "& Han", "")),
      model.type == "mod.sensitivity", list(c("Sensitivity", "", "")),
      model.type == "mod.best.ml.lu", list(c("LU-ML", "", "")),
      model.type == "mod.sensitivity.best.ml.lu", list(c("LU-ML,", "Sensitivity", "")), 
      model.type == "mod.ols", list(c("", "", ""))
    )] %>%
    .[, specification := fifelse(model.type == "mod.ols", "OLS", "IV")] %>%
    .[, cbsa.fe := "\\checkmark"]

  # --- 2. Filter Output ---
  DT.reg.melted <- DT.reg.output %>%
    .[ind.controls == TRUE & prediction.controls == TRUE & 
        fixed.effects != "date.division"] %>%
    melt(
      id.vars = c("startyr", "fixed.effects", "buildable.land.controls"),
      measure.vars = patterns("^mod"), 
      variable.name = "model.type", variable.factor = FALSE,
      value.name = "model"
    )

  # --- 3. Panel Construction Function ---
  f_tex_panel <- function(data.startyr) {
    DT.mods <- DT.reg.melted[startyr == c(data.startyr)] %>%
      merge(DT.models.lkp,
            by = c("fixed.effects", "buildable.land.controls", "model.type")) %>%
      .[order(mod.order)]
    
    # Rename RHS variables for stargazer
    DT.mods[model.type == "mod.ols", model := lapply(model, star_change_felm_rhs_names,
                                                     old = "lhpi_a", new = "xvar")]
    DT.mods[model.type != "mod.ols", model := lapply(model, star_change_felm_rhs_names,
                                                     old = "`lhpi_a(fit)`", new = "xvar")]
    
    if (length(DT.mods$model.type) != nrow(DT.models.lkp)) {
      stop("Model count mismatch in table generation.")
    } 

    mod.list <- DT.mods$model %>%
      lapply(star_change_felm_rhs_names, old = "`lhpi_a(fit)`", new = "xvar") %>%
      lapply(star_change_felm_rhs_names, old = "lhpi_a", new = "xvar")

    # Stats Extraction
    f_get_stage1_partial_r2 <- function(m) {
      partial_r2 <- m$stage1.r2
      if (is.null(partial_r2)) return(NA_real_)
      return(partial_r2)
    }

    f_get_stage1_fstat <- function(m) {
      fstat <- m$stage1$iv1fstat[[1]][["F"]]
      if (is.null(fstat)) return(NA_real_)
      return(fstat)
    }

    stage1.partial.r2 <- sapply(mod.list, f_get_stage1_partial_r2) %>%
      sprintf("%.02f", .) %>% gsub("NA", " ", .) %>%
      paste0(., collapse = " & ") %>% 
      paste0("First Stage Partial $R^2$ & ", ., " \\\\")
    
    stage1.fstat <- sapply(mod.list, f_get_stage1_fstat) %>%
      sprintf("%.02f", .) %>% gsub("NA", " ", .) %>%
      paste0(., collapse = " & ") %>% 
      paste0("First Stage $F$-Stat & ", ., " \\\\")

    # Stargazer Call
    star.out <- stargazer(
      mod.list, type = "latex",
      title = r"(\textbf{2SLS Housing Wealth Elasticity Estimates as in \citet{GurenEtAl2021}})",
      label = "tab:hw_elast_lu", 
      keep = "xvar", keep.stat = "n"
    ) %>%
      star_rhs_names("xvar", line1 = "YoY Log Diff in", line2 = "House Prices") %>%
      sub("\\\\textit\\{Dependent variable:\\}", "YoY Log Diff in Retail Emp Per Capita", x = .) %>%
      .[!grepl("lq.*retail", x = .)]

    # Insert Stats
    obs.line <- star.out %>% grepl("^Observations", x = .) %>% which
    star.out <- star_insert_row(star.out,
                                string = c(stage1.fstat, stage1.partial.r2),
                                insert.after = obs.line) %>%
      .[!grepl("^Observations", x = .)]

    return(star.out)
  }

  # --- 4. Combine Panels ---
  star.panel <- lapply(c(1978, 1990, 2000), f_tex_panel) %>%
    star_panel(starlist = .,
               panel.names = c("1978--2017", "1990--2017", "2000--2017"),
               same.summary.stats = FALSE, 
               panel.label.fontface = "bold")

  # --- 5. Add Custom Rows ---
  f_get_iv_row <- function(index) {
    DT.models.lkp[["ivs"]] %>% lapply(\(x) x[index]) %>% do.call("c", args = .) %>%
      paste0(collapse = " & ")
  }

  iv.row1 <- f_get_iv_row(1) %>% paste0("Instrument & ", ., " \\\\")
  
  specification <- DT.models.lkp[["specification"]] %>% paste0(collapse = " & ") %>%
    paste0("Specification & ", ., " \\\\")
  cbsa.fe.row <- DT.models.lkp[["cbsa.fe"]] %>% paste0(collapse = " & ") %>%
    paste0("CBSA FE & ", ., " \\\\")
  region.date.fe.row <- DT.models.lkp[["date.region.fe"]] %>% paste0(collapse = " & ") %>%
    paste0("Region $\\times$ Date FE & ", ., " \\\\")
  num.cbsas.row <- DT.models.lkp[["num.cbsas"]] %>% paste0(collapse = " & ") %>%
    paste0("Num. CBSAs & ", ., " \\\\")

  star.panel <- star.panel %>%
    star_insert_row(
      string = c("\\\\[-2.0ex] \\hline \\\\[-2.0ex]",
                 specification,
                 iv.row1,
                 "\\\\[-1.8ex]", 
                 num.cbsas.row,
                 "\\\\[-1.8ex]",
                 cbsa.fe.row,
                 region.date.fe.row), 
      insert.after = 36 # Adjusted based on typical output length
    ) %>%
    star_notes_tex(
      note.type = "caption",
      note = r"(Columns (1) to (3) replicate \citet{GurenEtAl2021} using the equation $\Delta y_{i,r,t} = \psi_i + \xi_{r,t} +  \beta \Delta p_{i,r,t} + \Gamma X_{i,r,t} + \epsilon_{i,r,t}$. $\Delta y_{i,r,t}$ is the log annual change in quarterly retail employment per capita (a consumption proxy in year-over-year first-difference form) for CBSA $i$ in census region $r$ at time $t$. $\Delta p_{i,r,t}$ is the log annual change in quarterly house prices for CBSA $i$. $\psi_i$, $\xi_{r,t}$, and $X_{i,r,t}$ represent CBSA fixed effects, census region $\times$ time fixed effects, and other controls, such as industry shares, respectively. See \citet{GurenEtAl2021} for a full list of controls. Column (1) employs OLS, while columns (2) and (3) use the Saiz Elasticity and Sensitivity instruments, respectively. Column (4) uses the elasticity proxy from \citet{BaumSnowHan2024} and column (5) employs the LU-ML IV. Robust standard errors clustered by time and CBSA are in parentheses. One, two, or three asterisks represent statistical significance at the 10, 5, and 1 percent levels, respectively.)"
    )

  # --- 6. Write Output ---
  CLmisc::mkdir_p(dirname(output_tex))
  star_write_and_compile_pdf(
    star.panel,
    file = output_tex
  )
  
  return(output_tex)
}
