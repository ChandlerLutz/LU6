## targets/housing_units.R

box::use(
  data.table[...], magrittr[`%>%`], CLmisc[get_rnhgis_tst]
)

housing_unit_targets <- list(
  tar_target(
    dt_hu_blkgrp_tst,
    get_rnhgis_tst(name = "CM7", geog_levels = "blck_grp") %>% as.data.table()
  ),

  ## CBSAs 
  tar_map(
    values = tibble::tibble(
      yr = c(2020, 2022, 2023),
      cbsa_shp_sym = rlang::syms(c("cbsa_shp_2020", "cbsa_shp_2022", "cbsa_shp_2023")),
      ),
    names = "yr",
    tar_target(
      dt_hu_tst_cbsa,
      f_get_nhgis_cbsa_hu_from_blk_grp(
        dt_hu_blkgrp_tst = dt_hu_blkgrp_tst,
        dt_blck_grp_shp_2010 = shp_blkgrp_2010,
        dt_cbsa_shp = cbsa_shp_sym,
        dt_cz20 = cz20_shp,
        to_cbsa_yr = yr
      )
    )
  ),

  ## Countys 
  tar_map(
    values = tibble::tibble(
      yr = c(2020, 2023),
      cnty_shp_sym = rlang::syms(c("cnty_shp_2020", "cnty_shp_2023")),
      ),
    names = "yr",
    tar_target(
      dt_hu_tst_cnty,
      f_get_nhgis_cnty_hu_from_blk_grp(
        dt_hu_blkgrp_tst = dt_hu_blkgrp_tst,
        dt_blck_grp_shp_2010 = shp_blkgrp_2010,
        dt_cnty_shp = cnty_shp_sym,
        dt_cz20 = cz20_shp,
        to_cnty_yr = yr
      )
    )
  )
)
