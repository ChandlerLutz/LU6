## core/gmaps_utils.R

f_get_gmaps_amenities_with_shp <- function(file_raw_gmaps_amenity_demand_cbsa2023,
                                           cbsa_shp_2023) {

  box::use(
    data.table[...], magrittr[`%>%`], nanoparquet[read_parquet], CLmisc[select_by_ref],
    sf[st_transform]
  )
  
  dt_cbsa_ref <- copy(cbsa_shp_2023) %>%
    select_by_ref(c("GEOID", "geometry")) %>%
    setcolorder(c("GEOID", "geometry")) %>%
    .[, geometry := st_transform(geometry, crs = 5070)]
  
  
  dt_cbsa_amenities <- read_parquet(file_raw_gmaps_amenity_demand_cbsa2023) %>%
    setDT() %>% 
    select_by_ref(c("GEOID", "demand_index_hiking",
                    "demand_index_natural_amenity_recreation",
                    "demand_index_other_viewpoints",
                    "demand_index_water_amenity")) %>%
    .[, names(.SD) := lapply(.SD, \(x) log(1 + x)),
      .SDcols = patterns("demand_index")]

  dt_cbsa_amenities_shp <- merge(dt_cbsa_ref, dt_cbsa_amenities, by = "GEOID",
                                 all = TRUE) %>%
    setcolorder(c("GEOID", "demand_index_hiking",
                  "demand_index_natural_amenity_recreation",
                  "demand_index_other_viewpoints",
                  "demand_index_water_amenity",
                  "geometry"))

  stopifnot(
    nrow(dt_cbsa_amenities_shp[is.na(geometry) | sf::st_is_empty(geometry)]) == 0,
    nrow(dt_cbsa_amenities_shp[is.na(demand_index_hiking)]) == 0
  )

  return(dt_cbsa_amenities_shp)

}

f_get_regional_gmaps_amenity_demand <- function(dt_gmaps_amenity_demand, target_shp, 
                                                geog_level, geog_yr) {
  
  box::use(
    data.table[...], magrittr[`%>%`], nanoparquet[read_parquet],
    sf[st_as_sf, st_is_empty, st_transform, st_join, st_intersects, st_nearest_feature], 
    CLmisc[select_by_ref, pipe_check]
  )

  stopifnot(
    is.data.table(dt_gmaps_amenity_demand),
    c("GEOID", "geometry") %chin% names(dt_gmaps_amenity_demand)
  )
  
  dt_target_shp <- copy(target_shp) %>% 
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "GEOID", "geometry")) %>%
    setcolorder(c("GISJOIN", "GEOID", "geometry")) %>%
    setnames("GISJOIN", "target_gjoin") %>%
    setnames("GEOID", "target_geoid") %>% 
    .[, geometry := st_transform(geometry, crs = 5070)]
  
  stopifnot(names(dt_target_shp) == c("target_gjoin", "target_geoid", "geometry"))
  
  dt_regional_amenities <- st_join(
    st_as_sf(dt_target_shp),
    st_as_sf(dt_gmaps_amenity_demand),
    join = st_intersects,
    left = FALSE, largest = TRUE
  ) %>%
    as.data.table()


  dt_leftover_target <- dt_target_shp %>%
    .[!target_geoid %in% dt_regional_amenities$target_geoid]

  if(nrow(dt_leftover_target) > 0) {
    dt_leftover_target <- dt_leftover_target %>%
    st_as_sf() %>%
    st_join(
      st_as_sf(dt_gmaps_amenity_demand),
      join = st_nearest_feature,
      left = TRUE, 
    ) %>% as.data.table()

    dt_regional_amenities <- rbind(dt_regional_amenities,
                                   dt_leftover_target,
                                   use.names = TRUE)
  }
  
  dt_regional_amenities <- dt_regional_amenities %>%
    .[, `:=`(
      geog_level = geog_level,
      geog_yr = geog_yr,
      region_geog_level = "cbsa",
      region_geog_yr = 2023
    )] %>%
    setnames("GEOID", "region_geoid") %>%
    setorder("target_gjoin") %>%
    select_by_ref(c("target_gjoin", "target_geoid", "geog_level", "geog_yr", 
                    "region_geog_level", "region_geoid", "region_geog_yr", 
                    "demand_index_hiking", "demand_index_natural_amenity_recreation",
                    "demand_index_other_viewpoints", "demand_index_water_amenity")) %>%
    .[, gmaps_region_amenity_demand := rowMeans(.SD, na.rm = TRUE),
      .SDcols = patterns("^demand_index")]
    
  cols_out <- c("target_gjoin", "target_geoid", "geog_level", "geog_yr", 
                "region_geog_level", "region_geoid", "region_geog_yr",
                "gmaps_region_amenity_demand")

  dt_regional_amenities <- dt_regional_amenities %>%
    select_by_ref(cols_out) %>%
    setcolorder(cols_out)

  stopifnot(dt_regional_amenities[, .N, by = .(target_gjoin)][, all(N == 1)])

  return(dt_regional_amenities)

}
