## core/housing_units.R

f_get_nhgis_cbsa_hu_from_blk_grp <- function(
  dt_hu_blkgrp_tst, dt_blck_grp_shp_2010, dt_cbsa_shp, dt_cz20_shp, to_cbsa_yr
) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref, pipe_check], sf[...]
  )

  stopifnot(
    is.data.table(dt_hu_blkgrp_tst), is.data.table(dt_blck_grp_shp_2010),
    is.data.table(dt_cbsa_shp), is.data.table(dt_cz20_shp)
  )
  
  dt_hu <- copy(dt_hu_blkgrp_tst) %>%
    setDT() %>%
    setnames("GISJOIN", "GISJOIN_blck_grp") %>%
    .[, names(.SD) := NULL, .SDcols = patterns("L$|U$")] %>%
    melt(id.vars = "GISJOIN_blck_grp", measure.vars = patterns("CM7AA"),
         variable.name = "year", value.name = "hu") %>%
    .[, year := sub("CM7AA", "", year)] %>%
    .[, year := as.integer(year)]

  dt_blck_grp_shp <- copy(dt_blck_grp_shp_2010) %>%
    select_by_ref(c("GISJOIN", "geometry")) %>%
    .[, geometry := st_transform(geometry, 5070)] %>%
    setnames("GISJOIN", "GISJOIN_blck_grp") %>%
    st_as_sf()
  
  dt_cbsa_shp <- copy(dt_cbsa_shp) %>%
    .[, geometry := st_transform(geometry, 5070)] %>%
    select_by_ref(c("GISJOIN", "GEOID", "geometry")) %>%
    setnames("GISJOIN", "GISJOIN_cbsa") %>%
    setnames("GEOID", "GEOID_cbsa") %>%
    st_as_sf()

  dt_cz20_shp <- copy(dt_cz20_shp) %>%
    .[, geometry := st_transform(geometry, crs = 5070)] %>%
    select_by_ref(c("cz20", "geometry")) %>%
    st_as_sf() %>%
    st_join(x = st_as_sf(dt_cbsa_shp), y = ., join = st_intersects,
            left = FALSE, largest = TRUE) %>%
    as.data.table() %>%
    select_by_ref(c("GEOID_cbsa", "cz20")) %>%
    pipe_check(.[duplicated(GEOID_cbsa)] %>% nrow() == 0)

  dt_cw_blkgrp2010_cbsa2020_cw <- st_join(
    x = dt_blck_grp_shp,
    y = dt_cbsa_shp,
    join = st_intersects,
    left = TRUE, largest = TRUE
  ) %>% as.data.table() %>%
    .[, geometry := NULL]

  dt_hu_cbsa <- merge(dt_hu, dt_cw_blkgrp2010_cbsa2020_cw,
                      by = "GISJOIN_blck_grp") %>%
    .[!is.na(GEOID_cbsa)] %>%
    .[, .(hu = sum(hu, na.rm = TRUE)), by = .(GEOID_cbsa, GISJOIN_cbsa, year)] %>%
    .[, cbsa_census_yr := c(to_cbsa_yr)] %>%
    merge(dt_cz20_shp, by = "GEOID_cbsa", all.x = TRUE) %>%
    setnames("GEOID_cbsa", "GEOID") %>%
    setnames("GISJOIN_cbsa", "GISJOIN") %>%
    .[order(GEOID, year)] %>%
    .[, dlog_hu := log(hu) - log(shift(hu, 1L)), by = GEOID] %>%
    .[, index := as.Date(paste0(year, "-01-01"))] %>%
    setcolorder(c("GEOID", "GISJOIN", "cbsa_census_yr", "cz20", "year", "index",
                  "hu", "dlog_hu"))

  return(dt_hu_cbsa)
}


f_get_nhgis_cnty_hu_from_blk_grp <- function(
  dt_hu_blkgrp_tst, dt_blck_grp_shp_2010, dt_cnty_shp, dt_cz20_shp, to_cnty_yr
) {
  
  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[select_by_ref, pipe_check], sf[...]
  )

  stopifnot(
    is.data.table(dt_hu_blkgrp_tst), is.data.table(dt_blck_grp_shp_2010),
    is.data.table(dt_cnty_shp), is.data.table(dt_cz20_shp)
  )
  
  dt_hu <- copy(dt_hu_blkgrp_tst) %>%
    setDT() %>%
    setnames("GISJOIN", "GISJOIN_blck_grp") %>%
    .[, names(.SD) := NULL, .SDcols = patterns("L$|U$")] %>%
    melt(id.vars = "GISJOIN_blck_grp", measure.vars = patterns("CM7AA"),
         variable.name = "year", value.name = "hu") %>%
    .[, year := sub("CM7AA", "", year)] %>%
    .[, year := as.integer(year)]

  dt_blck_grp_shp <- copy(dt_blck_grp_shp_2010) %>%
    select_by_ref(c("GISJOIN", "geometry")) %>%
    .[, geometry := st_transform(geometry, 5070)] %>%
    setnames("GISJOIN", "GISJOIN_blck_grp") %>%
    st_as_sf()

  dt_cnty_shp <- copy(dt_cnty_shp) %>%
    .[, geometry := sf::st_transform(geometry, 5070)] %>%
    select_by_ref(c("GISJOIN", "GEOID", "geometry")) %>%
    setnames("GEOID", "GEOID_cnty") %>%
    setnames("GISJOIN", "GISJOIN_cnty") %>%
    st_as_sf()

  dt_cz20 <- copy(dt_cz20_shp) %>%
    .[, geometry := st_transform(geometry, crs = 5070)] %>%
    select_by_ref(c("cz20", "geometry")) %>%
    st_as_sf() %>%
    st_join(x = dt_cnty_shp, y = ., join = st_intersects,
            left = FALSE, largest = TRUE) %>%
    as.data.table() %>%
    select_by_ref(c("GEOID_cnty", "cz20")) %>%
    pipe_check(.[duplicated(GEOID_cnty)] %>% nrow()== 0)

  dt_cw_blkgrp2010_cnty_to_cnty_cw <- st_join(
    x = dt_blck_grp_shp,
    y = dt_cnty_shp,
    join = st_intersects,
    left = TRUE, largest = TRUE
  ) %>% as.data.table() %>%
    .[, geometry := NULL]

  dt_hu_cnty <- merge(dt_hu, dt_cw_blkgrp2010_cnty_to_cnty_cw,
                      by = "GISJOIN_blck_grp") %>%
    .[!is.na(GEOID_cnty)] %>%
    .[, .(hu = sum(hu, na.rm = TRUE)), by = .(GEOID_cnty, GISJOIN_cnty, year)] %>%
    .[, cnty_census_yr := c(to_cnty_yr)] %>%
    merge(dt_cz20, by = "GEOID_cnty", all.x = TRUE) %>%
    setnames("GEOID_cnty", "GEOID") %>%
    setnames("GISJOIN_cnty", "GISJOIN") %>%
    .[order(GEOID, year)] %>%
    .[, dlog_hu := log(hu) - log(shift(hu, 1L)), by = GEOID] %>%
    .[, index := as.Date(paste0(year, "-01-01"))] %>%
    setcolorder(c("GEOID", "GISJOIN", "cnty_census_yr", "cz20", "year", "index",
                  "hu", "dlog_hu"))

  return(dt_hu_cnty)
}
