## core/fred_utils.R

#' Fetch Data from FRED
#'
#' @param symbols Character vector of FRED series IDs.
#' @return A data.table with columns 'index' (Date) and the requested series.
fetch_fred_data <- function(symbols) {
  box::use(
    pdfetch[pdfetch_FRED],
    data.table[as.data.table, setnames]
  )
  
  # Fetch and convert to data.table with "index" as the date column
  dt <- pdfetch_FRED(symbols) %>% 
    as.data.table(keep.rownames = "index")
  
  return(dt)
}

#' Calculate Real Mortgage Rate
#'
#' Calculates the real mortgage rate by subtracting YoY inflation (CPI) 
#' from the nominal 30-year mortgage rate.
#'
#' @param dt_mtg_raw Raw data.table for MORTGAGE30US.
#' @param dt_cpi_raw Raw data.table for CPIAUCSL.
calc_real_mtg_rate <- function(dt_mtg_raw, dt_cpi_raw) {
  box::use(
    data.table[...],
    lubridate[floor_date, year],
    magrittr[`%>%`]
  )

  # 1. Process Mortgage Rate (Weekly -> Monthly Average)
  dt_mtg_rate <- copy(dt_mtg_raw) %>%
    .[, .(mtgrate = mean(MORTGAGE30US, na.rm = TRUE)), 
      by = .(index = floor_date(index, unit = "month"))]

  # 2. Process CPI and Calculate Inflation
  dt_infl <- copy(dt_cpi_raw) %>%
    .[, index := floor_date(index, unit = "month")] %>%
    setorder(index) %>%
    # Calculate Year-over-Year Inflation
    .[, infl := (CPIAUCSL - shift(CPIAUCSL, 12)) / shift(CPIAUCSL, 12) * 100] %>%
    .[!is.na(infl), .(index, infl)]

  # 3. Merge and Calculate Real Rate
  dt_real <- merge(dt_mtg_rate, dt_infl, by = "index") %>%
    .[, real_mtg_rate := mtgrate - infl] %>%
    .[, .(index, year = year(index), real_mtg_rate)] %>%
    .[, d_yoy_real_mtg_rate := real_mtg_rate - shift(real_mtg_rate, 12)]

  return(dt_real)
}
