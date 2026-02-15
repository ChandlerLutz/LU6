## targets/fred.R

fred_targets <- list(
  # -- FRED Data -- #
  tar_target(
    dt_fred_mortgage,
    fetch_fred_data("MORTGAGE30US"),
    format = "parquet"
  ),
  
  tar_target(
    dt_fred_cpi,
    fetch_fred_data("CPIAUCSL"),
    format = "parquet"
  ),
  
  tar_target(
    dt_real_mtg_rate,
    calc_real_mtg_rate(dt_fred_mortgage, dt_fred_cpi),
    format = "parquet"
  )
)

