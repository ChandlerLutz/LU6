
f_est_guren_etal_tab1_models <- function(file_path) {
  box::use(
    magrittr[`%>%`], fst[read_fst], data.table[...], lfe[felm],
    fixest[feols, xpd], CLmisc[reduce_felm_object_size],
    starpolishr[star_change_felm_rhs_names]
  )

  DT_raw <- read_fst(file_path, as.data.table = TRUE)

  f_add_stage1_r2_to_felm_model <- function(felm.model) {
    stage1.summary <- summary(felm.model$stage1)
    felm.model$stage1.r2 <- stage1.summary$r2
    return(felm.model)
  }

  f_est <- function(startyr, DT_in) {
    DT <- copy(DT_in)
    DT <- DT[year(date) >= startyr]

    ind.share.vars.to.drop <- data.table(
      ind.share.var = (names(DT) %>% .[grepl("dateX.share", x = .)])
    ) %>%
      .[, year := gsub(".*([0-9]{4})_[0-9]{2}_[0-9]{2}$", "\\1", x = ind.share.var)] %>%
      .[as.numeric(year) < startyr] %>%
      .[, ind.share.var]

    if (length(ind.share.vars.to.drop) > 0) {
      DT[, c(ind.share.vars.to.drop) := NULL]
    }

    DT <- DT %>%
      .[, region.char := as.character(region)] %>%
      .[, date.char := as.character(date)] %>%
      .[, date.region.char := paste0(date.char, "_", region.char)]

    DT <- DT %>%
      .[, z_saiz := lhpi_usa_a * elasticity]

    DT.saiz <- DT[!is.na(z_saiz)]

    f_get_residualized_vars <- function(DT.reg, vars) {
      f_residualize_using_date_fe <- function(lhs.var) {
        formula <- xpd(~ predict_control + pc_additional_1 + pc_additional_2 +
          ..("dateX.share") | date.char, lhs = lhs.var)
        feols(formula, data = DT.reg) %>%
          .["residuals"]
      }

      f_residualize_using_date_region_fe <- function(lhs.var) {
        formula <- xpd(~ predict_control + pc_additional_1 + pc_additional_2 +
          ..("dateX.share") | date.char^region.char, lhs = lhs.var)
        feols(formula, data = DT.reg) %>%
          .["residuals"]
      }

      DT.out <- DT.reg[, .(date.char, cbsa.char, region.char)]

      for (var in vars) {
        resid.var.name.date.fe <- paste0(var, ".resid.date.fe")
        set(DT.out, j = resid.var.name.date.fe, value = f_residualize_using_date_fe(var))

        resid.var.name.date.region.fe <- paste0(var, ".resid.date.region.fe")
        set(DT.out, j = resid.var.name.date.region.fe, value = f_residualize_using_date_region_fe(var))
      }
      return(DT.out)
    }

    DT.full.residualized <- f_get_residualized_vars(
      DT,
      vars = c("lq_retail_a_nobad_pc", "lhpi_a", "z")
    )

    DT.saiz.residualized <- f_get_residualized_vars(
      DT.saiz,
      vars = c("lq_retail_a_nobad_pc", "lhpi_a", "z", "z_saiz")
    )

    mod.guren.ols <- felm(
      y.var ~ x.var | 0 | 0 | date.char + cbsa.char,
      DT.full.residualized[, .(
        date.char, cbsa.char,
        y.var = lq_retail_a_nobad_pc.resid.date.region.fe,
        x.var = lhpi_a.resid.date.region.fe
      )]
    ) %>%
      reduce_felm_object_size

    mod.guren.sensitivity <- felm(
      y.var ~ 1 | 0 | (x.var ~ z) | date.char + cbsa.char,
      DT.full.residualized[, .(
        date.char, cbsa.char,
        y.var = lq_retail_a_nobad_pc.resid.date.region.fe,
        x.var = lhpi_a.resid.date.region.fe,
        z = z.resid.date.region.fe
      )]
    ) %>%
      f_add_stage1_r2_to_felm_model(.) %>%
      star_change_felm_rhs_names(old = "`x.var(fit)`", new = "x.var") %>%
      reduce_felm_object_size

    mod.guren.saiz <- felm(
      y.var ~ 1 | 0 | (x.var ~ z) | date.char + cbsa.char,
      DT.saiz.residualized[, .(
        date.char, cbsa.char,
        y.var = lq_retail_a_nobad_pc.resid.date.fe,
        x.var = lhpi_a.resid.date.fe,
        z = z_saiz.resid.date.fe
      )]
    ) %>%
      f_add_stage1_r2_to_felm_model(.) %>%
      star_change_felm_rhs_names(old = "`x.var(fit)`", new = "x.var") %>%
      reduce_felm_object_size

    return(
      list(
        startyr = startyr,
        guren.et.al.tab1 = list(
          mod.guren.ols = mod.guren.ols,
          mod.guren.sensitivity = mod.guren.sensitivity,
          mod.guren.saiz = mod.guren.saiz
        )
      )
    )
  }

  mod.list.out <- list(
    mods.startyr.1978 = f_est(1978, DT_raw),
    mods.startyr.1990 = f_est(1990, DT_raw),
    mods.startyr.2000 = f_est(2000, DT_raw)
  )

  return(mod.list.out)
}


create_guren_etal_tab1_tex <- function(mod_list_all, file_out) {
  box::use(
    stargazer[stargazer], starpolishr[...],
    magrittr[`%>%`], utils[capture.output]
  )

  f_tex_guren_tab1_replication <- function(startyr) {
    mod.list <- mod_list_all[[paste0("mods.startyr.", startyr)]][["guren.et.al.tab1"]]

    capture.output(stargazer(mod.list, type = "text", keep = "x.var", keep.stat = "n"))

    star.out <- stargazer(
      mod.list,
      type = "latex",
      title = r"(\textbf{Replication of \citet{GurenEtAl2021}, Table 1})",
      label = "tab:guren_etal_tab1_rep",
      keep = "x.var",
      keep.stat = "n"
    ) %>%
      star_lhs_names("y.var", "YoY Log Diff in Retail Emp Per Capita") %>%
      star_rhs_names(
        pattern = "x.var",
        line1 = " YoY Log Diff in ",
        line2 = " HP Growth "
      )

    stage1.fstat <- c(
      " ",
      mod.list$mod.guren.sensitivity$stage1$iv1fstat$x.var[["F"]] %>% sprintf("%.02f", .),
      mod.list$mod.guren.saiz$stage1$iv1fstat$x.var[["F"]] %>% sprintf("%.02f", .)
    ) %>%
      paste0(collapse = " & ") %>%
      paste0("First Stage $F$-Stat & ", ., " \\\\")

    stage1.partial.r2 <- c(
      " ",
      mod.list$mod.guren.sensitivity$stage1.r2 %>% sprintf("%.02f", .),
      mod.list$mod.guren.saiz$stage1.r2 %>% sprintf("%.02f", .)
    ) %>%
      paste0(collapse = " & ") %>%
      paste0("First Stage Partial $R^2$ & ", ., " \\\\")

    star.out <- star.out %>%
      star_insert_row(string = c(stage1.fstat, stage1.partial.r2), insert.after = 18)

    return(star.out)
  }

  star.panel <- lapply(c(1978, 1990, 2000), f_tex_guren_tab1_replication) %>%
    star_panel(
      starlist = .,
      panel.names = c("1978--2017", "1990--2017", "2000--2017"),
      same.summary.stats = FALSE,
      panel.label.fontface = "bold"
    )

  specification.row <- c("OLS", "IV", "IV") %>%
    paste0(collapse = " & ") %>% paste("Specification & ", ., " \\\\")
  instruments.row <- c(" ", "Sensitivity", "Saiz Elast") %>%
    paste0(collapse = " & ") %>% paste("Instrument & ", ., " \\\\")
  num.cbsas.row <- c("380", "380", "270") %>%
    paste0(collapse = " & ") %>%
    paste0("Num. CBSAs & ", ., " \\\\")

  fe.cbsa.row <- rep("$\\checkmark$", star_ncol(star.panel) - 1) %>%
    paste0(collapse = " & ") %>%
    paste0("CBSA FE & ", ., " \\\\")
  yq.fe.row <- c(" ", " ", "\\checkmark") %>%
    paste0(collapse = " & ") %>%
    paste0("Yr-Qtr FE & ", ., " \\\\")
  region.yq.fe.row <- c("\\checkmark", " \\checkmark", " ") %>%
    paste0(collapse = " & ") %>%
    paste0("Region, Yr-Qtr FE & ", ., " \\\\")

  star.panel <- star.panel %>%
    star_insert_row(
      string = c(
        "\\\\[-2.0ex] \\hline \\\\[-2.0ex]",
        specification.row,
        instruments.row,
        "\\\\[-1.8ex]",
        num.cbsas.row,
        "\\\\[-1.8ex]",
        yq.fe.row, region.yq.fe.row, fe.cbsa.row
      ),
      insert.after = 40
    ) %>%
    star_notes_tex(
      note.type = "caption",
      note = r"(Replication of \citet{GurenEtAl2021}, table 1 using the equation $\Delta y_{i,r,t} = \psi_i + \xi_{r,t} +  \beta \Delta p_{i,r,t} + \Gamma X_{i,r,t} + \epsilon_{i,r,t}$. $\Delta y_{i,r,t}$ is the log annual change in quarterly retail employment per capita (a consumption proxy in year-over-year first-difference form) for CBSA $i$ in census region $r$ at time $t$. $\Delta p_{i,r,t}$ is the log annual change in quarterly house prices for CBSA $i$. $\psi_i$, $\xi_{r,t}$, and $X_{i,r,t}$ represent CBSA fixed effects, census region $\times$ time fixed effects, and other controls, such as industry shares, respectively. See \citet{GurenEtAl2021} for a full list of controls. Robust standard errors clustered by time and CBSA are in parentheses. One, two, or three asterisks represent statistical significance at the 10, 5, and 1 percent levels, respectively.)"
    )

  dir.create(dirname(file_out), showWarnings = FALSE, recursive = TRUE)
  star_write_and_compile_pdf(star.panel, file = file_out)

  return(file_out)
}


est_gurenetal_sc_lu_ml_reg_models <- function(file_guren_replicate, dt_lu_ml,
                                              dt_bsh) {
  box::use(
    fst[read_fst],
    data.table[...],
    lfe[felm],
    fixest[feols, xpd],
    magrittr[`%>%`],
    CLmisc[reduce_felm_object_size]
  )

  # -- 1. Data Prep -- #
  
  # Prep ML Data
  # Fix: Select ONLY keys and prediction column to avoid 'lhpi_a' duplication in merge
  DT.lu.ml <- copy(dt_lu_ml) %>%
    setnames("index", "date", skip_absent = TRUE) %>%
    .[, cbsa := as.numeric(cbsa)] 
  
  # Only keep the prediction column and keys. 
  # Note: The user code later expects "lu_ml_xgboost" to be renamed to "z_lu_ml"
  cols_ml_keep <- intersect(names(DT.lu.ml), c("cbsa", "date", "lu_ml_xgboost"))
  DT.lu.ml <- DT.lu.ml[, ..cols_ml_keep]

  # Prep BSH Data
  DT.bsh <- copy(dt_bsh) %>%
    setnames("GEOID_metro", "cbsa") %>%
    .[, cbsa := as.numeric(cbsa)] %>%
    .[, .(cbsa, gamma01b_space_FMM)]

  # Prep Guren Data
  DT <- read_fst(file_guren_replicate, as.data.table = TRUE) %>%
    .[, region.char := as.character(region)] %>%
    .[, date.char := as.character(date)] %>% 
    .[, date.region.char := paste0(date.char, "_", region.char)]

  # Merge
  DT <- DT %>%
    merge(DT.lu.ml, by = c("cbsa", "date"), all.x = TRUE) %>%
    merge(DT.bsh, by = "cbsa", all.x = TRUE) %>%
    .[, z_saiz := elasticity * lhpi_usa_a] %>%
    .[, z_saiz_unaval := unaval * lhpi_usa_a] %>%
    .[, z_bsh := gamma01b_space_FMM * lhpi_usa_a] %>%
    setnames("lu_ml_xgboost", "z_lu_ml", skip_absent = TRUE)

  # Keep/Delete Vars to save memory
  vars.to.keep <- c(
    "cbsa", "date",
    "lq_retail_a_nobad_pc", "lhpi_a", 
    "predict_control", "pc_additional_1", "pc_additional_2",
    "date.char", "cbsa.char", "date.region.char", "region.char", "division", 
    "bl.ntile", # Needed for optional control
    names(DT) %>% .[grepl("^dateX", x = .)],
    names(DT) %>% .[grepl("^z", x = .)]
  )
  vars.to.delete <- names(DT) %>% .[!(. %chin% vars.to.keep)]
  
  if (length(vars.to.delete) > 0) {
    DT[, c(vars.to.delete) := NULL]
  }

  # Only use cbsas in the LU data
  DT <- DT[!is.na(z_lu_ml)]

  # -- 2. Define Helper Functions -- #

  f_add_stage1_r2_to_felm_model <- function(felm.model) {
    stage1.summary <- summary(felm.model$stage1)
    felm.model$stage1.r2 <- stage1.summary$r2
    return(felm.model)
  }

  # -- 3. Regression Logic -- #

  DT.reg.options <- expand.grid(
    startyr = c(1978, 1990, 2000),
    ind.controls = c(TRUE), 
    prediction.controls = c(TRUE),
    buildable.land.controls = c(FALSE),
    fixed.effects = c("date", "date.region"),
    stringsAsFactors = FALSE
  ) %>% setDT() %>%
    .[, id := seq_len(.N)] %>%
    setcolorder(c("id"))

  f_run_regressions <- function(reg.options.id) {
    
    DT.opts <- DT.reg.options[id == c(reg.options.id)]
    startyr <- DT.opts$startyr

    DT.tmp <- DT[year(date) >= startyr]

    ## Drop the industry control variables that are associated with dates before startyr
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

    ## The controls 
    controls <- c("1")
    DT.names <- names(DT.tmp)
    if (DT.opts$ind.controls == TRUE)
      controls <- c(controls, DT.names %>% .[grepl("dateX.share", x = .)])
    if (DT.opts$prediction.controls == TRUE)
      controls <- c(controls, DT.names %>% .[grepl("predict_control|pc_additional",x = .)])

    ## The FE
    fe <- "date.char"
    if (DT.opts$fixed.effects == "date.region") {
      fe <- paste0(fe, " ^ region.char")
    } else if (DT.opts$fixed.effects == "date.division") {
      fe <- paste0(fe, " ^ division")
    }
    if (DT.opts$buildable.land.controls == TRUE)
      fe <- paste0(fe, " + bl.ntile")

    # Define variables to residualize
    vars.to.residualize = DT.names %>%
      .[grepl("^z", x = .)] %>%
      .[!(. %chin% c("z_saiz", "z_saiz_unaval"))] %>%
      c("lq_retail_a_nobad_pc", "lhpi_a", .)
    
    vars.to.residualize.saiz <- c("lq_retail_a_nobad_pc", "lhpi_a", "z_saiz", "z_saiz_unaval")
    vars.to.residualize.bsh <- c("lq_retail_a_nobad_pc", "lhpi_a", "z_bsh")

    f_residualize <- function(lhs.vars, DT_resid_in) {
      formula <- xpd(..lhs.vars ~ ..controls | ..fe,
                     ..lhs.vars = paste0("c(", paste0(lhs.vars, collapse = ", "), ")"), 
                     ..controls = controls,
                     ..fe = fe
      )
      
      DT.out <- feols(formula, data = DT_resid_in) %>%
        lapply(function(m) m[["residuals"]]) %>%
        do.call("cbind", args = .) %>%
        as.data.table %>% setnames(names(.), lhs.vars) %>%
        cbind(DT_resid_in[, .(cbsa.char, date.char)], .)
        
      return(DT.out)
    }

    DT.residualized <- f_residualize(lhs.vars = vars.to.residualize, DT.tmp)
    DT.residualized.saiz <- f_residualize(lhs.vars = vars.to.residualize.saiz, DT.tmp.saiz)
    DT.residualized.bsh <- f_residualize(lhs.vars = vars.to.residualize.bsh, DT.tmp.bsh)

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
      # Run feols to get lean stats
      f.feols <- xpd(
        lq_retail_a_nobad_pc ~ 1 | 0 | lhpi_a ~ ..ivs,
        ..ivs = ivs
      )
      mod.felm <- felm(f.felm, data = DT_reg) %>%
        f_add_stage1_r2_to_felm_model %>% 
        reduce_felm_object_size
      
      mod.feols <- feols(f.feols, data = DT_reg, lean = TRUE)
      
      mod.felm$iv_wh <- mod.feols$iv_wh
      if ("iv_sargan" %in% names(mod.feols))
        mod.felm$iv_sargan <- mod.feols$iv_sargan

      return(mod.felm)
    }

    mod.ols <- f_reg_ols(DT.residualized)
    mod.sensitivity <- f_reg(ivs = "z", DT.residualized)
    mod.saiz <- f_reg(ivs = "z_saiz", DT.residualized.saiz)
    # mod.saiz.unaval <- f_reg(ivs = "z_saiz_unaval", DT.residualized.saiz)
    mod.bsh <- f_reg(ivs = "z_bsh", DT.residualized.bsh)
    mod.best.ml.lu <- f_reg(ivs = c("z_lu_ml"), DT.residualized)
    mod.sensitivity.best.ml.lu <- f_reg(ivs = c("z_lu_ml", "z"), DT.residualized)

    DT.out <- data.table(
      reg.options.id = c(reg.options.id),
      mod.ols = list(mod.ols),
      mod.sensitivity = list(mod.sensitivity),
      mod.saiz = list(mod.saiz),
      # mod.saiz.unaval = list(mod.saiz.unaval), 
      mod.bsh = list(mod.bsh),
      mod.best.ml.lu = list(mod.best.ml.lu),
      mod.sensitivity.best.ml.lu = list(mod.sensitivity.best.ml.lu)
    )
    return(DT.out)
  }

  # -- 4. Run All Regressions -- #
  DT.reg.output <- lapply(DT.reg.options$id, f_run_regressions) %>%
    rbindlist

  DT.reg.output <- merge(DT.reg.options, DT.reg.output,
                         by.x = "id", by.y = "reg.options.id")

  return(DT.reg.output)
}

extract_gurenetal_sc_lu_ml_reg_mod_stats <- function(dt_reg_output) {
  box::use(
    data.table[...],
    magrittr[`%>%`]
  )
  
  DT.mod.stats <- dt_reg_output[fixed.effects == "date.region"]

  vars.to.delete <- DT.mod.stats %>% names %>% .[grepl("controls$", x = .)] %>% c(., "fixed.effects")

  f_get_iv_sargan_p <- function(m) {
    if (is.null(m$iv_sargan)) return(NA_real_)
    return(m$iv_sargan$p)
  }
  f_get_stage1_fstat <- function(model) {
    stage1.fstat <- model$stage1$iv1fstat[[1]]["F"]
    if (is.null(stage1.fstat)) return(NA_real_)
    return(stage1.fstat)
  }
  f_get_stage1_r2 <- function(model) {
    stage1.r2 <- model$stage1.r2
    if (is.null(stage1.r2)) return(NA_real_)
    return(stage1.r2)
  }

  DT.out <- DT.mod.stats %>%
    .[, c(vars.to.delete) := NULL] %>%
    melt(id.vars = c("id", "startyr"), variable.name = "model.type", variable.factor = FALSE,
         value.name = "model") %>%
    .[, num.obs :=  sapply(model, function(m) m$N)] %>%
    .[, stage1.fstat := sapply(model, f_get_stage1_fstat)] %>%
    .[, stage1.r2 := sapply(model, f_get_stage1_r2)] %>%
    .[, iv.sargan := sapply(model, f_get_iv_sargan_p)] %>%
    .[, model := NULL]
  
  return(DT.out)
}


create_gurenetal_sc_lu_reg_tex <- function(dt_reg_output, file_out) {
  box::use(
    magrittr[`%>%`], data.table[...],
    stargazer[stargazer], starpolishr[...],
    utils[capture.output]
  )

  # -- 1. Define Lookup Table -- #
  # Defines the order and metadata for the columns in the table
  DT.models.lkp <- data.table(
    fixed.effects = "date.region",
    buildable.land.controls = FALSE,
    model.type = c("mod.ols", "mod.saiz", "mod.sensitivity", "mod.bsh", "mod.best.ml.lu")
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

  # -- 2. Process Regression Output -- #
  # Filter to relevant models and reshape to long format
  DT.reg <- dt_reg_output %>%
    .[ind.controls == TRUE & prediction.controls == TRUE & fixed.effects != "date.division"] %>%
    melt(
      id.vars = c("startyr", "fixed.effects", "buildable.land.controls"),
      measure.vars = patterns("^mod"),
      variable.name = "model.type", variable.factor = FALSE,
      value.name = "model"
    )

  # -- 3. Helper Functions -- #
  f_get_stage1_partial_r2 <- function(m) {
    if (is.null(m$stage1.r2)) return(NA_real_)
    return(m$stage1.r2)
  }

  f_get_stage1_fstat <- function(m) {
    if (is.null(m$stage1) || is.null(m$stage1$iv1fstat)) return(NA_real_)
    fstat <- m$stage1$iv1fstat[[1]][["F"]]
    if (is.null(fstat)) return(NA_real_)
    return(fstat)
  }

  f_get_over_id_pval <- function(felm.model) {
    if (is.null(felm.model[["iv_sargan"]])) return(" ")
    return(sprintf("%.02f", felm.model[["iv_sargan"]][["p"]]))
  }

  # -- 4. Panel Generation Function -- #
  f_tex_panel <- function(data.startyr) {
    
    # Filter for year and merge with lookup to enforce column order
    DT.mods <- DT.reg[startyr == data.startyr] %>%
      merge(DT.models.lkp, by = c("fixed.effects", "buildable.land.controls", "model.type")) %>%
      setorder(mod.order)

    if (nrow(DT.mods) != nrow(DT.models.lkp)) {
      missing <- setdiff(DT.models.lkp$model.type, DT.mods$model.type)
      stop(paste("Missing models in output for year", data.startyr, ":", paste(missing, collapse = ", ")))
    }

    mod.list <- DT.mods$model

    # Standardize RHS names for stargazer
    mod.list <- lapply(mod.list, function(m) {
      m <- star_change_felm_rhs_names(m, old = "`lhpi_a(fit)`", new = "xvar")
      m <- star_change_felm_rhs_names(m, old = "lhpi_a", new = "xvar")
      return(m)
    })

    # Calculate Stats Rows
    stage1.partial.r2 <- sapply(mod.list, f_get_stage1_partial_r2) %>%
      sprintf("%.02f", .) %>% gsub("NA", " ", .) %>%
      paste0(collapse = " & ") %>% paste0("First Stage Partial $R^2$ & ", ., " \\\\")

    stage1.fstat <- sapply(mod.list, f_get_stage1_fstat) %>%
      sprintf("%.02f", .) %>% gsub("NA", " ", .) %>%
      paste0(collapse = " & ") %>% paste0("First Stage $F$-Stat & ", ., " \\\\")

    overid.pval <- sapply(mod.list, f_get_over_id_pval) %>%
      paste0(collapse = " & ") %>% paste0("OverID p-value & ", ., " \\\\")

    # Capture dummy text output
    capture.output(stargazer(mod.list, type = "text", keep = "xvar", keep.stat = "n"))

    # Generate Latex Panel
    star.out <- stargazer(
      mod.list, type = "latex",
      title = r"(\textbf{2SLS Housing Wealth Elasticity Estimates as in \citet{GurenEtAl2021}})",
      label = "tab:hw_elast_lu",
      keep = "xvar", keep.stat = "n"
    ) %>%
      star_rhs_names("xvar", line1 = "YoY Log Diff in", line2 = "House Prices") %>%
      sub("\\\\textit\\{Dependent variable:\\}", "YoY Log Diff in Retail Emp Per Capita", x = .) %>%
      .[!grepl("lq.*retail", x = .)] # Remove default dependent var line

    # Insert Stats Rows
    obs.line <- grep("^Observations", star.out)[1]
    star.out <- star_insert_row(star.out, string = c(stage1.fstat, stage1.partial.r2), insert.after = obs.line) %>%
      .[!grepl("^Observations", x = .)]

    return(star.out)
  }

  # -- 5. Create and Combine Panels -- #
  star.panel <- lapply(c(1978, 1990, 2000), f_tex_panel) %>%
    star_panel(
      starlist = .,
      panel.names = c("1978--2017", "1990--2017", "2000--2017"),
      same.summary.stats = FALSE,
      panel.label.fontface = "bold"
    )

  # -- 6. Add Bottom Matter -- #
  f_get_iv_row <- function(index) {
    DT.models.lkp$ivs %>% lapply(function(x) x[index]) %>% do.call("c", args = .) %>%
      paste0(collapse = " & ")
  }

  iv.row1 <- paste0("Instrument & ", f_get_iv_row(1), " \\\\")
  
  specification <- paste0("Specification & ", paste0(DT.models.lkp$specification, collapse = " & "), " \\\\")
  cbsa.fe.row <- paste0("CBSA FE & ", paste0(DT.models.lkp$cbsa.fe, collapse = " & "), " \\\\")
  region.date.fe.row <- paste0("Region $\\times$ Date FE & ", paste0(DT.models.lkp$date.region.fe, collapse = " & "), " \\\\")
  num.cbsas.row <- paste0("Num. CBSAs & ", paste0(DT.models.lkp$num.cbsas, collapse = " & "), " \\\\")

  star.panel <- star.panel %>%
    star_insert_row(
      string = c(
        "\\\\[-2.0ex] \\hline \\\\[-2.0ex]",
        specification,
        iv.row1,
        "\\\\[-1.8ex]",
        num.cbsas.row,
        "\\\\[-1.8ex]",
        cbsa.fe.row,
        region.date.fe.row
      ),
      insert.after = 36
    ) %>%
    star_notes_tex(
      note.type = "caption",
      note = r"(Columns (1) to (3) replicate \citet{GurenEtAl2021} using the equation $\Delta y_{i,r,t} = \psi_i + \xi_{r,t} +  \beta \Delta p_{i,r,t} + \Gamma X_{i,r,t} + \epsilon_{i,r,t}$. $\Delta y_{i,r,t}$ is the log annual change in quarterly retail employment per capita (a consumption proxy in year-over-year first-difference form) for CBSA $i$ in census region $r$ at time $t$. $\Delta p_{i,r,t}$ is the log annual change in quarterly house prices for CBSA $i$. $\psi_i$, $\xi_{r,t}$, and $X_{i,r,t}$ represent CBSA fixed effects, census region $\times$ time fixed effects, and other controls, such as industry shares, respectively. See \citet{GurenEtAl2021} for a full list of controls. Column (1) employs OLS, while columns (2) and (3) use the Saiz Elasticity and Sensitivity instruments, respectively. Column (4) uses the elasticity proxy from \citet{BaumSnowHan2024} and column (5) employs the LU-ML Price Pressure Index. Robust standard errors clustered by time and CBSA are in parentheses. One, two, or three asterisks represent statistical significance at the 10, 5, and 1 percent levels, respectively.)"
    )

  dir.create(dirname(file_out), showWarnings = FALSE, recursive = TRUE)
  star_write_and_compile_pdf(star.panel, file = file_out)

  return(file_out)
}
