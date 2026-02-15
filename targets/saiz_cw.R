## targets/saiz_cw.R

saiz_cw_targets <- list(

  ## Saiz in CBSAs
  tar_map(
    values = tibble::tibble(
      year = c(2010, 2013:2023),
      shp = rlang::syms(paste0("cbsa_shp_", c(2010, 2013:2023)))
    ),
    names = "year",
    tar_target(
      saiz_cbsa,
      f_cw_saiz_to_cbsa(dt_saiz = msa_shp_1999, cbsa_shp = shp)
    )
  )
)
