## core/reg_income_elast_hp_utils.R

f_est_income_elasticity_hu <- function(est_label,
                                       dt_lu_ml_hu, file_bea_income,
                                       dt_bsh, dt_saiz, geog_cluster_var) {

  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref, reduce_felm_object_size],
    lfe[felm], fixest[xpd], broom[tidy, glance],
  )

  dt_bea_income <- readRDS(file_bea_income) %>%
    select_by_ref(c("GEOID", "year", "PersonalIncome_Thousands")) %>%
    setcolorder(c("GEOID", "year", "PersonalIncome_Thousands")) %>%
    .[, year := as.integer(year)] %>%
    .[year %in% c(1990, 2000, 2010, 2020)] %>%
    .[order(GEOID, year)] %>%
    .[, dlog_income := log(PersonalIncome_Thousands) -
          log(shift(PersonalIncome_Thousands, 1L)), by = GEOID] %>%
    .[!is.na(dlog_income)] %>%
    select_by_ref(c("GEOID", "year", "dlog_income")) %>%
    setcolorder(c("GEOID", "year", "dlog_income"))

  stopifnot(
    c("GEOID", "index", "dlog_hu", geog_cluster_var, "lu_ml_xgboost") %chin%
      names(dt_lu_ml_hu),
    is.data.table(dt_bsh),
    c("GEOID", "gamma01b_space_FMM") %in% names(dt_bsh),
    c("GEOID", "saiz_elasticity") %in% names(dt_saiz)
  )

  dt_bsh <- copy(dt_bsh) %>%
    select_by_ref(c("GEOID", "gamma01b_space_FMM")) %>%
    setnames("gamma01b_space_FMM", "bsh_elasticity") %>%
    .[, bsh_std := (bsh_elasticity - mean(bsh_elasticity, na.rm = TRUE)) / sd(bsh_elasticity, na.rm = TRUE)] %>%
    .[, bsh_less_constrained := as.integer(bsh_elasticity > mean(bsh_elasticity,
                                                                 na.rm = TRUE))]

  dt_saiz <- copy(dt_saiz) %>%
    select_by_ref(c("GEOID", "saiz_elasticity")) %>%
    .[, saiz_std := (saiz_elasticity - mean(saiz_elasticity, na.rm = TRUE)) / sd(saiz_elasticity, na.rm = TRUE)] %>%
    .[, saiz_less_constrained := as.integer(saiz_elasticity > mean(saiz_elasticity,
                                                                   na.rm = TRUE))]


  dt_est <- copy(dt_lu_ml_hu) %>%
    setnames("lu_ml_xgboost", "lu_ml_hu") %>%
    .[, lu_ml_less_constrained := as.integer(lu_ml_hu > mean(lu_ml_hu, na.rm = TRUE)),
      by = year] %>%
    .[, lu_ml_hu_std := (lu_ml_hu - mean(lu_ml_hu, na.rm = TRUE)) / sd(lu_ml_hu,
                                                                       na.rm = TRUE),
      by = year] %>%
    merge(dt_bea_income, by = c("GEOID", "year"), all.x = TRUE) %>%
    .[, year_char := as.character(year)] %>%
    merge(dt_bsh, by = "GEOID", all.x = TRUE) %>%
    merge(dt_saiz, by = "GEOID", all.x = TRUE) %>%
    .[, geog_cluster_var := ..geog_cluster_var,
      env = list(..geog_cluster_var = geog_cluster_var)] %>%
    .[, geog_cluster_var := as.character(geog_cluster_var)] %>%
    .[index >= "2010-01-01"]

  f_reg <- function(sc_var, fe_string) {

    f_formula <- xpd(dlog_hu ~ ..sc_var * dlog_income | ..fe_string | 0 |
                       geog_cluster_var,
                     ..sc_var = sc_var,
                     ..fe_string = fe_string)

    felm(f_formula, dt_est) 
  }

  dt_mod_list_out <- expand.grid(
    sc_var = c("lu_ml_less_constrained", "bsh_less_constrained",
               "saiz_less_constrained"),
    fe_string = c("year_char", paste0(c("GEOID", "year_char"), collapse = " + ")),
    stringsAsFactors = FALSE
  ) %>%
    as.data.table() %>%
    rbind(
      data.table(
        sc_var = c("lu_ml_hu_std", "lu_ml_hu_std"),
        fe_string = c("year_char", paste0(c("GEOID", "year_char"), collapse = " + "))
      ),
      data.table(
        sc_var = c("bsh_std", "bsh_std"),
        fe_string = c("year_char", "GEOID + year_char")
      ),
      data.table(
        sc_var = c("saiz_std", "saiz_std"),
        fe_string = c("year_char", "GEOID + year_char")
      )
    ) %>%
    .[, mod := Map(f_reg, sc_var, fe_string)] %>%
    .[, tidy_mod := lapply(mod, tidy)] %>%
    .[, tidy_mod := lapply(tidy_mod, as.data.table)] %>%
    .[, glance_mod := lapply(mod, glance)] %>%
    .[, glance_mod := lapply(glance_mod, as.data.table)] %>%
    .[, mod := lapply(mod, reduce_felm_object_size)] %>%
    .[, est_label := c(est_label)] %>%
    setcolorder("est_label") %>%
    .[, beta1 := sapply(tidy_mod, function(x) x[term == "dlog_income", estimate])] %>%
    .[, beta1_se := sapply(tidy_mod, function(x) x[term == "dlog_income",
                                                   std.error])] %>%
    .[, beta1_t := sapply(tidy_mod, function(x) x[term == "dlog_income", statistic])] %>%
    .[, beta1_p := sapply(tidy_mod, function(x) x[term == "dlog_income", p.value])] %>%
    .[, beta3 := sapply(
      tidy_mod, function(x) x[grepl("(lu_ml|bsh|saiz).*:dlog_income", term),
                              estimate]
    )] %>%
    .[, beta3_se := sapply(
      tidy_mod, function(x) x[grepl("(lu_ml|bsh|saiz).*:dlog_income", term),
                              std.error]
    )] %>%
    .[, beta3_t := sapply(
      tidy_mod, function(x) x[grepl("(lu_ml|bsh|saiz).*:dlog_income", term),
                              statistic]
    )] %>%
    .[, beta3_p := sapply(
      tidy_mod, function(x) x[grepl("(lu_ml|bsh|saiz).*:dlog_income", term),
                              p.value]
    )]
  
  return(dt_mod_list_out)
}

f_est_income_elast_hp <- function(est_label, dt_hp_panel, file_bea_income,
                                  dt_lu_ml_hu, dt_bsh, dt_saiz, 
                                  geog_cluster_var,
                                  dt_cw_to_st, 
                                  dt_geoids_to_keep = NULL) {

  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref, reduce_felm_object_size],
    lfe[felm], fixest[xpd], broom[tidy, glance]
  )

  dt_bea_income <- readRDS(file_bea_income) %>%
    select_by_ref(c("GEOID", "year", "PersonalIncome_Thousands")) %>%
    setcolorder(c("GEOID", "year", "PersonalIncome_Thousands")) %>%
    .[, year := as.integer(year)] %>%
    .[year %in% c(1990, 2000, 2010, 2020)] %>%
    .[order(GEOID, year)] %>%
    .[, dlog_income := log(PersonalIncome_Thousands) -
          log(shift(PersonalIncome_Thousands, 1L)), by = GEOID] %>%
    .[!is.na(dlog_income)] %>%
    select_by_ref(c("GEOID", "year", "dlog_income")) %>%
    setcolorder(c("GEOID", "year", "dlog_income"))

  stopifnot(
    is.data.table(dt_hp_panel),
    is.data.table(dt_lu_ml_hu),
    c("GEOID", "index", "dlog_hu", geog_cluster_var, "lu_ml_xgboost") %chin%
      names(dt_lu_ml_hu), 
    is.data.table(dt_bsh),
    c("GEOID", "gamma01b_space_FMM") %in% names(dt_bsh),
    is.data.table(dt_saiz),
    c("GEOID", "saiz_elasticity") %in% names(dt_saiz),
    is.data.table(dt_cw_to_st),
    c("GEOID", "STUSPS") %in% names(dt_cw_to_st)
  )

  if (!is.null(dt_geoids_to_keep)) {

    stopifnot(
      is.data.table(dt_geoids_to_keep),
      c("GEOID") %in% names(dt_geoids_to_keep)
    )
    
    geoids_to_keep <- unique(dt_geoids_to_keep$GEOID)
    dt_hp_panel <- dt_hp_panel[GEOID %chin% c(geoids_to_keep)]
    dt_lu_ml_hu <- dt_lu_ml_hu[GEOID %chin% c(geoids_to_keep)]
  }

  
  dt_hp <- copy(dt_hp_panel) %>%
    .[, year := year(index)] %>%
    .[year %in% c(1990, 2000, 2010, 2020)] %>%
    .[order(GEOID, index)] %>%
    ## .[, .SD[.N], by = .(GEOID, year)] %>%
    select_by_ref(c("GEOID", "year", "hp_local")) %>%
    .[, .(hp_local = mean(hp_local, na.rm = TRUE)), by = .(GEOID, year)] %>%
    .[order(GEOID, year)] %>%
    .[, dlog_hp_local := log(hp_local) - log(shift(hp_local, 1L)), by = GEOID] %>%
    .[!is.na(dlog_hp_local)] %>%
    select_by_ref(c("GEOID", "year", "dlog_hp_local"))

  dt_lu_ml_hu <- copy(dt_lu_ml_hu) %>%
    .[, year := year(index)] %>%
    .[year %in% c(1990, 2000, 2010, 2020)] %>%
    .[order(GEOID, index)] %>%
    .[, .SD[.N], by = .(GEOID, year)] %>%
    setnames("lu_ml_xgboost", "lu_ml_hu") %>%
    select_by_ref(c("GEOID", "cz20", "year", "dlog_hu", "lu_ml_hu")) %>%
    .[order(GEOID, year)] %>%
    .[, lu_ml_less_constrained := as.integer(lu_ml_hu > mean(lu_ml_hu,
                                                             na.rm = TRUE)),
      by = year] %>%
    .[, lu_ml_hu_std := (lu_ml_hu - mean(lu_ml_hu, na.rm = TRUE)) /
              sd(lu_ml_hu, na.rm = TRUE), by = year]

  dt_cw_to_st <- copy(dt_cw_to_st) %>%
    select_by_ref(c("GEOID", "STUSPS")) %>%
    setnames("STUSPS", "stabb")

  dt_bsh <- copy(dt_bsh) %>%
    select_by_ref(c("GEOID", "gamma01b_space_FMM")) %>%
    setnames("gamma01b_space_FMM", "bsh_elasticity") %>%
    .[, bsh_less_constrained := as.integer(bsh_elasticity > mean(bsh_elasticity,
                                                                 na.rm = TRUE))] %>%
    .[, bsh_std := (bsh_elasticity - mean(bsh_elasticity, na.rm = TRUE)) /
          sd(bsh_elasticity, na.rm = TRUE)]
  

  dt_saiz <- copy(dt_saiz) %>%
    select_by_ref(c("GEOID", "saiz_elasticity")) %>%
    .[, saiz_less_constrained := as.integer(saiz_elasticity > mean(saiz_elasticity,
                                                                   na.rm = TRUE))] %>%
    .[, saiz_std := (saiz_elasticity - mean(saiz_elasticity, na.rm = TRUE)) /
          sd(saiz_elasticity, na.rm = TRUE)]
  
  
  dt_est <- merge(dt_lu_ml_hu, dt_hp, by = c("GEOID", "year"), all.x = TRUE) %>%
    merge(dt_bea_income, by = c("GEOID", "year"), all.x = TRUE) %>%
    .[, year_char := as.character(year)] %>%
    merge(dt_cw_to_st, by = "GEOID", all.x = TRUE) %>%
    merge(dt_bsh, by = "GEOID", all.x = TRUE) %>%
    merge(dt_saiz, by = "GEOID", all.x = TRUE) %>%
    .[, geog_cluster_var := ..geog_cluster_var,
      env = list(..geog_cluster_var = geog_cluster_var)] %>%
    .[, geog_cluster_var := as.character(geog_cluster_var)]

  f_reg <- function(sc_var, fe_string) {

    f_formula <- xpd(dlog_hp_local ~ ..sc_var * dlog_income | ..fe_string | 0 |
                       geog_cluster_var,
                     ..sc_var = sc_var,
                     ..fe_string = fe_string)

    felm(f_formula, dt_est) 
  }

  dt_mod_list_out <- expand.grid(
    sc_var = c("lu_ml_less_constrained", "bsh_less_constrained",
               "saiz_less_constrained"),
    fe_string = c("year_char", paste0(c("GEOID", "year_char"), collapse = " + ")),
    stringsAsFactors = FALSE
  ) %>%
    as.data.table() %>%
    rbind(
      data.table(
        sc_var = c("lu_ml_hu_std", "lu_ml_hu_std", "lu_ml_hu_std", "lu_ml_hu_std"),
        fe_string = c("year_char", "GEOID + year_char", "cz20 + year_char",
                      "stabb + year_char")
      ),
      data.table(
        sc_var = c("bsh_std", "bsh_std"),
        fe_string = c("year_char", "GEOID + year_char")
      ),
      data.table(
        sc_var = c("saiz_std", "saiz_std"),
        fe_string = c("year_char", "GEOID + year_char")
      )
    ) %>% 
    .[, mod := Map(f_reg, sc_var, fe_string)] %>%
    .[, tidy_mod := lapply(mod, tidy)] %>%
    .[, tidy_mod := lapply(tidy_mod, as.data.table)] %>%
    .[, glance_mod := lapply(mod, glance)] %>%
    .[, glance_mod := lapply(glance_mod, as.data.table)] %>%
    .[, mod := lapply(mod, reduce_felm_object_size)] %>%
    .[, est_label := c(est_label)] %>%
    setcolorder("est_label") %>%
    .[, beta1 := sapply(tidy_mod, function(x) x[term == "dlog_income", estimate])] %>%
    .[, beta1_se := sapply(tidy_mod, function(x) x[term == "dlog_income",
                                                   std.error])] %>%
    .[, beta1_t := sapply(tidy_mod, function(x) x[term == "dlog_income", statistic])] %>%
    .[, beta1_p := sapply(tidy_mod, function(x) x[term == "dlog_income", p.value])] %>%
    .[, beta3 := sapply(
      tidy_mod, function(x) x[grepl("(lu_ml|bsh|saiz).*:dlog_income", term),
                              estimate]
    )] %>%
    .[, beta3_se := sapply(
      tidy_mod, function(x) x[grepl("(lu_ml|bsh|saiz).*:dlog_income", term),
                              std.error]
    )] %>%
    .[, beta3_t := sapply(
      tidy_mod, function(x) x[grepl("(lu_ml|bsh|saiz).*:dlog_income", term),
                              statistic]
    )] %>%
    .[, beta3_p := sapply(
      tidy_mod, function(x) x[grepl("(lu_ml|bsh|saiz).*:dlog_income", term),
                              p.value]
    )]

  return(dt_mod_list_out)
}


f_make_income_elast_intro_fig <- function(dt_mods_hp, dt_mods_hu, fe_string,
                                          output_plot) {

  box::use(
    data.table[...], ggplot2[...], magrittr[`%>%`],
    CLmisc[theme_cowplot_cl, mkdir_p],
    ggrepel[geom_text_repel], patchwork[...]
  )

  stopifnot(
    is.data.table(dt_mods_hp), is.data.table(dt_mods_hu),
    c("est_label", "beta1", "beta1_se") %in% names(dt_mods_hp),
    c("est_label", "beta1", "beta1_se") %in% names(dt_mods_hu),
    is.character(fe_string), length(fe_string) == 1L,
    fe_string %chin% c("year_char", "GEOID + year_char"),
    fe_string %chin% dt_mods_hp$fe_string,
    fe_string %chin% dt_mods_hu$fe_string,
    is.character(output_plot), length(output_plot) == 1L
  )
  fe_string_tmp <- fe_string

  dt_mods_hp <- dt_mods_hp %>%
    .[grepl("less_constrained$", sc_var) & fe_string == c(fe_string_tmp)]
  dt_mods_hu <- dt_mods_hu %>%
    .[grepl("less_constrained$", sc_var) & fe_string == c(fe_string_tmp)]

  dt_top_income_elast_hp <- dt_mods_hp[, .(sc_var, beta1, beta3)] %>%
    .[, less_constrained_elasticity := beta1 + beta3] %>%
    .[, more_constrained_elasticity :=  beta1] %>%
    .[, sc_var := gsub("_less_constrained", "", sc_var)] %>%
    .[, sc_label := fcase(
        sc_var == "lu_ml", "LU-ML Supply\nIndex",
        sc_var == "bsh", "Baum-Snow\n& Han",
        sc_var == "saiz", "Saiz"
    )] %>%
    .[, sc_label := factor(sc_label, levels = c("Baum-Snow\n& Han", "Saiz",
                                                "LU-ML Supply\nIndex"))] %>%
    melt(id.vars = c("sc_var", "sc_label"),
         measure.vars = c("less_constrained_elasticity",
                          "more_constrained_elasticity"),
         variable.name = "constraint_level", value.name = "elasticity") %>%
    .[, constraint_level := fcase(
      constraint_level == "less_constrained_elasticity", "Less Constrained CBSAs",
      constraint_level == "more_constrained_elasticity", "More Constrained CBSAs"
    )]

  income_elast_hp_plot_subtitle <- fcase(
        fe_string == "year_char", "Decadal log differences, with time fixed effects",
        fe_string == "GEOID + year_char", "Decadal log differences, with CBSA and time fixed effects"
          )
  p_top_income_elast_hp <- ggplot(dt_top_income_elast_hp,
                   aes(x = sc_label, y = elasticity, fill = constraint_level)) +
    geom_col(position = position_dodge(width = 0.5), width = 0.5, alpha = 0.8) +
    geom_text(aes(label = sprintf("%.02f", elasticity)),
              position = position_dodge(width = 0.5),
              vjust = -0.25, size = 3) +
    scale_fill_manual(values = c("Less Constrained CBSAs" = "darkblue",
                                 "More Constrained CBSAs" = "darkred")) +
    scale_y_continuous(limits = c(0, max(dt_top_income_elast_hp$elasticity) * 1.1),
                       expand = c(0, 0),
                       breaks = seq(0, 1.5, by = 0.1)) +
    labs(y = "Decadal % Δ in HP per 1% Δ in Income",
         title = "A: Income Elasticity of House Prices, 1990-2020",
         subtitle = income_elast_hp_plot_subtitle) +
    theme_cowplot_cl() +
    theme(axis.title.x = element_blank(),
          legend.title = element_blank(),
          legend.position = "inside",
          legend.position.inside = c(0.2, 0.93),
          plot.margin = margin(t = 0, r = 0, b = 0, l = 0, unit = "pt"))

  dt_middle_income_elast_est_diff_hp <- dt_mods_hp %>%
    .[, .(sc_var, beta3, beta3_se, beta3_t)] %>%
    .[, beta3_ci_lower := beta3 - 2 * beta3_se] %>%
    .[, beta3_ci_upper := beta3 + 2 * beta3_se] %>%
    .[, sc_var := gsub("_less_constrained", "", sc_var)] %>%
    .[, sc_label := fcase(
      sc_var == "lu_ml", "LU-ML Supply\nIndex",
      sc_var == "bsh", "Baum-Snow\n& Han",
      sc_var == "saiz", "Saiz"
    )] %>%
    .[, sc_label := factor(sc_label, levels = c("Baum-Snow\n& Han", "Saiz",
                                                "LU-ML Supply\nIndex"))] %>%
    .[, text_label := paste0(
    sprintf("%0.3f", round(beta3, 3)), "\n",
     "\U1D461 = ", sprintf("%0.3f", round(beta3_t, 3))
  )]


  p_middle_income_elast_hp_est_diff <- ggplot(
    dt_middle_income_elast_est_diff_hp, aes(x = sc_label, y = beta3)
  ) +
    geom_hline(yintercept = 0, linetype = "solid", color = "gray50", alpha = 0.25,
               linewidth = 1.01) +
    geom_pointrange(aes(y = beta3, ymin = beta3_ci_lower, ymax = beta3_ci_upper),
                    shape = 21, color = "black", fill = "white", size = 1.01,
                    linewidth = 1.01) +
    geom_text_repel(aes(label = text_label), size = 3.5, seed = 42, nudge_y = 0.02,
                    nudge_x = 0.25) +
    scale_y_continuous(breaks = seq(-1, 1, by = 0.25)) +
    labs(title = "B: Difference in Income Elasticity of House Prices",
         subtitle = "Less Constrained \u2013 More Constrained") + 
    theme_cowplot_cl() +
    theme(axis.title = element_blank())


  dt_bottom_income_elast_hu_est_diff <- dt_mods_hu %>% 
    .[, .(sc_var, beta3, beta3_se, beta3_t)] %>%
    .[, beta3_ci_lower := beta3 - 2 * beta3_se] %>%
    .[, beta3_ci_upper := beta3 + 2 * beta3_se] %>%
    .[, sc_var := gsub("_less_constrained", "", sc_var)] %>%
    .[, sc_label := fcase(
      sc_var == "lu_ml", "LU-ML Supply\nIndex",
      sc_var == "bsh", "Baum-Snow\n& Han",
      sc_var == "saiz", "Saiz"
    )] %>%
    .[, sc_label := factor(sc_label, levels = c("Baum-Snow\n& Han", "Saiz",
                                                "LU-ML Supply\nIndex"))] %>%
    .[, text_label := paste0(
      sprintf("%0.3f", round(beta3, 3)), "\n",
      "\U1D461 = ", sprintf("%0.3f", round(beta3_t, 3))
    )]

  p_bottom_income_elast_hu_est_diff <- ggplot(
    dt_bottom_income_elast_hu_est_diff, aes(x = sc_label, y = beta3)
  ) +
    geom_hline(yintercept = 0, linetype = "solid", color = "gray50", alpha = 0.25,
               linewidth = 1.01) +
    geom_pointrange(aes(y = beta3, ymin = beta3_ci_lower, ymax = beta3_ci_upper),
                    shape = 21, color = "black", fill = "white", size = 1.01,
                    linewidth = 1.01) +
    geom_text_repel(aes(label = text_label), size = 3.5, seed = 42, nudge_y = 0.02,
                    nudge_x = 0.25) +
    scale_y_continuous(breaks = seq(-1, 1, by = 0.1)) +
    labs(title = "C: Difference in Income Elasticity of Housing Units",
         subtitle = "Less Constrained \u2013 More Constrained") + 
    theme_cowplot_cl() +
    theme(axis.title = element_blank())
                                    
  
  p_all <- p_top_income_elast_hp / p_middle_income_elast_hp_est_diff /
    p_bottom_income_elast_hu_est_diff +
    plot_layout(heights = c(2.75, 1, 1)) &
    theme(plot.margin = margin(t = 1, r = 0, b = 0, l = 1, unit = "pt"))

  mkdir_p(dirname(output_plot))
  ggsave(output_plot, p_all, device = cairo_pdf, width = 6, height = 7.5,
         units = "in", dpi = 900)
  

}


f_make_income_elast_beta_3_fig <- function(dt_mods_income_elast_hp_fmcc_cbsas_fmcc,
                                           dt_mods_income_elast_hp_fmcc_cbsas_fhfa,
                                           dt_mods_income_elast_hp_fhfa_cbsas,
                                           dt_mods_income_elast_hp_fhfa_cntys,
                                           fe_string, output_plot) {

  dt_mods_income_elast_hp_fmcc_cbsas_fmcc <-
    tar_read(mods_income_elast_hp_fmcc_cbsas_fmcc)
  dt_mods_income_elast_hp_fmcc_cbsas_fhfa <-
    tar_read(mods_income_elast_hp_fmcc_cbsas_fhfa)
  dt_mods_income_elast_hp_fhfa_cbsas <-
    tar_read(mods_income_elast_hp_fhfa_cbsas)
  dt_mods_income_elast_hp_fhfa_cntys <-
    tar_read(mods_income_elast_hp_fhfa_cntys)

  lu_ml_color <- "#619CFF"
  plot_palette <- c("#00BA38", "#F8766D", lu_ml_color)
  ceof_scale_breaks <-  seq(-0.5, 0.5, by = 0.1)
  

  box::use(
    data.table[...], ggplot2[...], magrittr[`%>%`],
    CLmisc[theme_cowplot_cl, mkdir_p],
    ggrepel[geom_text_repel], patchwork[...], cowplot[get_legend]
  )

  stopifnot(
    is.data.table(dt_mods_income_elast_hp_fmcc_cbsas_fmcc),
    is.data.table(dt_mods_income_elast_hp_fmcc_cbsas_fhfa),
    is.data.table(dt_mods_income_elast_hp_fhfa_cbsas),
    is.data.table(dt_mods_income_elast_hp_fhfa_cntys),
    c("est_label", "beta3", "beta3_se") %chin%
      names(dt_mods_income_elast_hp_fmcc_cbsas_fmcc),
    c("est_label", "beta3", "beta3_se") %chin%
      names(dt_mods_income_elast_hp_fmcc_cbsas_fhfa),
    c("est_label", "beta3", "beta3_se") %chin% names(dt_mods_income_elast_hp_fhfa_cbsas),
    c("est_label", "beta3", "beta3_se") %chin% names(dt_mods_income_elast_hp_fhfa_cntys),
    is.character(fe_string), length(fe_string) == 1L,
    fe_string %chin% dt_mods_income_elast_hp_fmcc_cbsas_fmcc$fe_string,
    fe_string %chin% dt_mods_income_elast_hp_fmcc_cbsas_fhfa$fe_string,
    fe_string %chin% dt_mods_income_elast_hp_fhfa_cbsas$fe_string,
    fe_string %chin% dt_mods_income_elast_hp_fhfa_cntys$fe_string,
    is.character(output_plot), length(output_plot) == 1L
  )

  fe_string_tmp <- fe_string

  dt_major_metros_binary <- rbind(
    dt_mods_income_elast_hp_fmcc_cbsas_fmcc,
    dt_mods_income_elast_hp_fmcc_cbsas_fhfa
  ) %>%
    .[grepl("less_constrained", sc_var) & fe_string == c(fe_string_tmp)] %>% 
    .[, est_label := fcase(
      est_label == "fmcc", "Freddie Mac",
      est_label == "fhfa", "FHFA"
    )] %>%
    .[, sc_var := fcase(
        grepl("lu_ml", sc_var), "LU-ML Supply Index",
        grepl("bsh", sc_var), "Baum-Snow & Han",
        grepl("saiz", sc_var), "Saiz"
    )] %>%
    .[, sc_var := factor(sc_var, levels = c("Baum-Snow & Han", "Saiz",
                                            "LU-ML Supply Index"))] %>%
    select_by_ref(c("est_label", "sc_var", "beta3", "beta3_se"))

  
  p_major_metros_binary <- ggplot(dt_major_metros_binary, aes(x = est_label)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "black") +
    geom_pointrange(aes(y = beta3, ymin = beta3 - 2 * beta3_se,
                        ymax = beta3 + 2 * beta3_se,
                        color = sc_var),
                    shape = 21, fill = "white", size = 0.5, linewidth = 1,
                    position = position_dodge(width = 0.25)) +
    scale_color_manual(values = plot_palette) +
    scale_y_continuous(limits = coef_scale_limits) +
    labs(title = "1A: Less \u2013 More Constrained",
         y = "Difference in Income Elasticity of House Prices") + 
    coord_flip() + 
    theme_cowplot_cl() +
    guides(color = guide_legend(title = "Supply Constraint Measure")) +
    theme(axis.title.y = element_blank(),
          axis.ticks.y = element_blank(),
          axis.line.y = element_blank(),
          legend.position = "bottom",
          legend.title.position = "top"
          )

  dt_major_metros_continuous <- rbind(
    dt_mods_income_elast_hp_fmcc_cbsas_fmcc,
    dt_mods_income_elast_hp_fmcc_cbsas_fhfa
  ) %>%
    .[!grepl("less_constrained", sc_var) & fe_string == c(fe_string_tmp)] %>% 
    .[, est_label := fcase(
      est_label == "fmcc", "Freddie Mac",
      est_label == "fhfa", "FHFA"
    )] %>%
    .[, sc_var := fcase(
      grepl("lu_ml", sc_var), "LU-ML Supply Index",
      grepl("bsh", sc_var), "Baum-Snow & Han",
      grepl("saiz", sc_var), "Saiz"
    )] %>%
    .[, sc_var := factor(sc_var, levels = c("Baum-Snow & Han", "Saiz",
                                            "LU-ML Supply Index"))] %>%
    select_by_ref(c("est_label", "sc_var", "beta3", "beta3_se"))

  p_major_metros_continuous <- ggplot(dt_major_metros_continuous, aes(x = est_label)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "black") +
    geom_pointrange(aes(y = beta3, ymin = beta3 - 2 * beta3_se,
                        ymax = beta3 + 2 * beta3_se,
                        color = sc_var),
                    shape = 21, fill = "white", size = 0.5, linewidth = 1,
                    position = position_dodge(width = 0.25)) +
    scale_color_manual(values = plot_palette) +
    scale_y_continuous(limits = coef_scale_limits) +
    labs(title = "1B: 1 SD Less Constrained",
         y = "Difference in Income Elasticity of House Prices") + 
    coord_flip() +
    theme_cowplot_cl() +
    guides(color = guide_legend(title = "Supply Constraint Measure")) +
    theme(axis.title.y = element_blank(),
          axis.ticks.y = element_blank(),
          axis.line.y = element_blank(),
          axis.text.y = element_blank(), 
          legend.position = "bottom",
          legend.title.position = "top"          
          )
    

  dt_all_us_binary <- rbind(
    dt_mods_income_elast_hp_fhfa_cbsas, 
    dt_mods_income_elast_hp_fhfa_cntys
  ) %>%
    .[sc_var == "lu_ml_less_constrained" & fe_string == c(fe_string_tmp)] %>%
    .[, est_label := fcase(
      est_label == "fhfa_cbsas", "CBSAs",
      est_label == "fhfa_cntys", "Counties"
    )] %>%
    .[, est_label := factor(est_label, levels = c("Counties", "CBSAs"))] %>%
    select_by_ref(c("est_label", "beta3", "beta3_se"))

  p_all_us_binary <- ggplot(dt_all_us_binary, aes(x = est_label)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "black") +
    geom_pointrange(aes(y = beta3, ymin = beta3 - 2 * beta3_se,
                        ymax = beta3 + 2 * beta3_se),
                    shape = 21, fill = "white", size = 0.5, linewidth = 1,
                    color = lu_ml_color) +
    scale_y_continuous(limits = coef_scale_limits) +
    labs(title = "2A: Less \u2013 More Constrained",
         y = "Difference in Income Elasticity of House Prices") + 
    coord_flip() +
    theme_cowplot_cl() +
    theme(axis.title.y = element_blank(),
          axis.ticks.y = element_blank(),
          axis.line.y = element_blank(),
          legend.position = "none")
    

  dt_all_us_continuous <- rbind(
    dt_mods_income_elast_hp_fhfa_cbsas, 
    dt_mods_income_elast_hp_fhfa_cntys
  ) %>%
    .[sc_var == "lu_ml_hu_std" & fe_string == c(fe_string_tmp)] %>%
    .[, est_label := fcase(
      est_label == "fhfa_cbsas", "CBSAs",
      est_label == "fhfa_cntys", "Counties"
    )] %>%
    .[, est_label := factor(est_label, levels = c("Counties", "CBSAs"))] %>%
    select_by_ref(c("est_label", "beta3", "beta3_se"))

  p_all_us_continuous <- ggplot(dt_all_us_continuous, aes(x = est_label)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "black") +
    geom_pointrange(aes(y = beta3, ymin = beta3 - 2 * beta3_se,
                        ymax = beta3 + 2 * beta3_se),
                    shape = 21, fill = "white", size = 0.5, linewidth = 1,
                    color = lu_ml_color) +
    scale_y_continuous(limits = coef_scale_limits) + 
    coord_flip() +
    labs(title = "2B: 1 SD Less Constrained",
         y = "Difference in Income Elasticity of House Prices") + 
    theme_cowplot_cl() +
    theme(axis.title.y = element_blank(),
          axis.ticks.y = element_blank(),
          axis.line.y = element_blank(),
          axis.text.y = element_blank(),
          legend.position = "none") 

  p_legend <- get_legend(p_major_metros_binary)

  p_major_metros_binary <- p_major_metros_binary + theme(legend.position = "none")
  p_major_metros_continuous <- p_major_metros_continuous +
    theme(legend.position = "none")
  
  p_panel_1 <- p_major_metros_binary + p_major_metros_continuous + 
    plot_layout(axis_titles = "collect") 
    

  p_panel_2 <- p_all_us_binary + p_all_us_continuous + 
    plot_layout(axis_titles = "collect")

  p_patchwork_title_theme <- theme_void() +
    theme(plot.title = element_text(size = 12, face = "bold", hjust = -0.1))

  p_title_panel_1 <- ggplot() + labs(title = "1: Major Metros by Dataset") +
    p_patchwork_title_theme

  p_title_panel_2 <- ggplot() + labs(title = "2: All U.S. Data by Geographic Level") +
    p_patchwork_title_theme

  p <- p_title_panel_1 /
    p_panel_1 / 
    p_title_panel_2 / 
    p_panel_2 /
    p_legend

  p <- p + 
    plot_layout(heights = c(0.1, 1, 0.1, 1, 0.35)) 
    
  
    


           
}
  
