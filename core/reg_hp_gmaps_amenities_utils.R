## core/reg_hp_gmaps_amenities_utils.R

f_reg_hp_gmaps_amenity_models <- function(dt_hp_panel, dt_gmaps_amenities, dt_hm_cycles) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref, reduce_felm_object_size],
    lfe[felm]
  )

  stopifnot(
    is.data.table(dt_hp_panel), is.data.table(dt_gmaps_amenities),
    is.data.table(dt_hm_cycles)
  )
  
  dt_gmaps <- copy(dt_gmaps_amenities) %>%
    select_by_ref(c("target_geoid", "gmaps_region_amenity_demand")) %>%
    setnames("target_geoid", "GEOID") %>%
    na.omit("gmaps_region_amenity_demand") %>%
    .[, gmaps_region_amenity_demand := (gmaps_region_amenity_demand -
                                          mean(gmaps_region_amenity_demand)) /
          sd(gmaps_region_amenity_demand)]
  
  dt <- copy(dt_hp_panel) %>%
    .[index <= "2024-12-31"] %>%
    .[dt_hm_cycles,
      on = .(index >= cycle_start_date, index <= cycle_end_date),
      hm_cycle := i.cycle_label] %>%
    .[!is.na(dlog_yoy_hp_local)] %>%
    merge(dt_gmaps, by = "GEOID") %>%
    .[, index_char := as.character(index)]

  mod_cycles <- felm(
    dlog_yoy_hp_local ~ gmaps_region_amenity_demand:hm_cycle | index_char | 0 | GEOID, 
    data = dt
  ) %>%
    reduce_felm_object_size()

  mod_full_sample <- felm(
    dlog_yoy_hp_local ~ gmaps_region_amenity_demand | index_char | 0 | GEOID, 
    data = dt
  ) %>%
    reduce_felm_object_size()

  mod_list_out <- list(
    mod_cycles = mod_cycles,
    mod_full_sample = mod_full_sample
  )

  return(mod_list_out)
}


f_plot_gmaps_amenity_regs <- function(mods_fmcc_cbsa, mods_fhfa_cbsa, mods_zillow_cbsa,
                                      output_plot) {
  
  box::use(
    data.table[...], ggplot2[...], broom[tidy], magrittr[`%>%`],
    CLmisc[mkdir_p, theme_cowplot_cl], ggtext[element_markdown]
  )

  stopifnot(
    inherits(mods_fmcc_cbsa$mod_cycles, "felm"),
    inherits(mods_fmcc_cbsa$mod_full_sample, "felm"),
    inherits(mods_fhfa_cbsa$mod_cycles, "felm"),
    inherits(mods_fhfa_cbsa$mod_full_sample, "felm"),
    inherits(mods_zillow_cbsa$mod_cycles, "felm"),
    inherits(mods_zillow_cbsa$mod_full_sample, "felm"),
    is.character(output_plot), length(output_plot) == 1L
  )

  f_clean <- function(mod_list) {

    dt_mod_cycles <- tidy(mod_list$mod_cycles) %>%
      as.data.table() %>%
      .[grepl("gmaps_region_amenity_demand:hm_cycle", term)] %>%
      .[, hm_cycle := gsub("gmaps_region_amenity_demand:hm_cycle", "", term)] %>%
      .[, hm_cycle := gsub("_", "-", hm_cycle)] %>%
      .[, term :=  NULL] %>%
      setcolorder(c("hm_cycle"))

    dt_mod_full_sample <- tidy(mod_list$mod_full_sample) %>%
      as.data.table() %>%
      .[term == "gmaps_region_amenity_demand"] %>%
      .[, hm_cycle := "**Full Sample**"] %>%
      .[, term := NULL] %>%
      setcolorder(c("hm_cycle"))

    return(rbind(dt_mod_cycles, dt_mod_full_sample, use.names = TRUE))

  }

  dt_fmcc <- f_clean(mods_fmcc_cbsa) %>%
    .[, model := "Freddie Mac"] %>%
    setcolorder(c("model"))

  dt_fhfa <- f_clean(mods_fhfa_cbsa) %>%
    .[, model := "FHFA"] %>%
    setcolorder(c("model"))

  dt_zillow <- f_clean(mods_zillow_cbsa) %>%
    .[, model := "Zillow"] %>%
    setcolorder(c("model"))

  dt_plot <- rbindlist(list(dt_fmcc, dt_fhfa, dt_zillow)) %>%
    .[, model := factor(model, levels = c("Freddie Mac", "FHFA", "Zillow"))] %>%
    .[, hm_cycle := factor(hm_cycle, levels = c("1970s", "1980s", "1990s",
                                                "2000-2007", "2008-2012", "2013-2019",
                                                "2020s", "**Full Sample**"))] %>%
    ## For interpretability, convert to percentage change in HP per SD change in gmaps demand
    .[, let(estimate = estimate * 100, std.error = std.error * 100)]

  p <- ggplot(dt_plot, aes(x = hm_cycle, y = estimate, group = model, color = model)) +
    geom_hline(yintercept = 0, linetype = "solid", color = "gray", linewidth = 1.01) +
    geom_vline(xintercept = 7.5, linetype = "dashed", color = "darkgray") +
    geom_errorbar(aes(ymin = estimate - 2 * std.error,
                      ymax = estimate + 2 * std.error),
                  width = 0.3, position = position_dodge(width = 0.4)) +
    geom_point(position = position_dodge(width = 0.4)) +
    scale_y_continuous(breaks = seq(-5, 5, by = 0.5)) +
    labs(
      y = "Annualized % Δ HP per\nSD Δ in Natural Amenity Demand",
      subtitle ="LHS Var: Log Annual Change in CBSA House Prices\nRHS Var: Standardized Google Maps Natural Amenity Demand", 
      color = "House Price Source") +
    theme_cowplot_cl() +
    theme(axis.text.x = element_markdown(angle = 45, hjust = 1),
          axis.title.x = element_blank(),
          legend.title = element_blank(),
          legend.position = "inside",
          legend.position.inside = c(0.1, 0.17)
          )

  mkdir_p(dirname(output_plot))
  ggsave(output_plot, plot = p, device = cairo_pdf, width = 7.1, height = 3.4, dpi = 600)
  
  
}
