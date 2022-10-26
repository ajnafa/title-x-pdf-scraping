table_builders <- function(.pages, .year, .month) {
  if (.year >= 2019) {
    out <- .data_table_builder_new(.pages)
  }
  
  else if (.year < 2019) {
    out <- .data_table_builder_old(.pages)
  }
  
  out <- out |>
    dplyr::mutate(
      year = .year,
      month = .month
    )
  
  return(out)
}

address_cleaning <- function(text_data, date) {
  
  # Initial pre-processing
  out <- text_data |>
    # Set empty cell values to NA
    mutate(across(matches("site_name|street|city|state"), ~ na_if(.x, ""))) |>
    # Drop NA values in site name
    drop_na("site_name")
  
  # Need to correct here for weird issues with parsing
  if (is.null(ncol(out$city)) & is.null(ncol(out$state))) {
    
    # Fill in missing values with lags where the parsing is weird
    out <- text_data |>
      mutate(
        across(
          c(street, city, state),
          ~ case_when(
            is.na(.x) ~ lead(.x),
            TRUE ~ .x
          )),
        issue = 0
      )
  }
  
  if (!is.null(ncol(out$city))) {
    out <- text_data |>
      mutate(
        # Coerce city to a single character
        city = as.character(city[, 2]),
        # Fill in missing values with lags where the parsing is weird
        across(
          c(street, city, state),
          ~ case_when(
            is.na(.x) ~ lead(.x),
            TRUE ~ .x
          )
        ))
  }
  
  # Fix a single messed up state column for Georgia
  if (!is.null(ncol(out$state))) {
    out <- text_data |>
      mutate(
        # Coerce city to a single character
        state = "GA",
        # Fill in missing values with lags where the parsing is weird
        across(
          c(street, city, state),
          ~ case_when(
            is.na(.x) ~ lead(.x),
            TRUE ~ .x
          )
        ))
  }
  
  out <- out |>
    # Group the data by street, city, and state
    group_by(street, city, state) |>
    # Append duplicate site names to fix sections with messed up parsing
    summarise(site_name = paste0(site_name, collapse = " ,")) |>
    # Ungroup the data
    ungroup() |>
    # Add month and year columns
    mutate(date = date) %>% 
    # Split the concatenated site names into two columns
    separate(
      col = site_name,
      into = c("site_name_a", "site_name_b"),
      sep = " ,"
    )
  
  return(out)
}


service_site_extract <- function(.pages, .year) {
  ## Check the first column for serve site identifiers
  if (.year >= 2019) {
    out <- .service_site_extract_new(.pages)
  }
  
  else if (.year < 2019) {
    out <- .service_site_extract_old(.pages)
  }
}

.service_site_extract_old <- function(.pages) {
  
  ## Check the first column for serve site identifiers
  site_rows <- stringr::str_which(.pages[1,], "Service Site")
  
  ## If service site rows was greater than 0 extract the site rows
  if (length(site_rows) > 0) {
    out <- .pages[, site_rows:ncol(.pages)]
  }
  
  else {
    out <- NULL
  }
  
  return(out)
}

.service_site_extract_new <- function(.pages) {
  
  ## Check the first column for serve site identifiers
  site_rows <- stringr::str_which(.pages[,1], "Service Site")
  
  ## If service site rows was blank, check the second column
  if (length(site_rows) == 0) {
    site_rows <- stringr::str_which(.pages[,2], "Service Site")
  }
  
  ## If service site rows was greater than 0 extract the site rows
  if (length(site_rows) > 0) {
    out <- .pages[c(1, site_rows),]
  }
  
  else {
    out <- NULL
  }
  
  return(out)
}

.data_table_builder_old <- function(.pages) {
  
  ## Get the street address
  address_pos <- stringr::str_which(.pages[1,], "Street Address")
  
  ## Get the street address
  city_pos <- stringr::str_which(.pages[1,], "City")
  
  ## Get the state
  state_pos <- stringr::str_which(.pages[1,], "State")
  
  ## Build the tibble
  out <- tibble::tibble(
    site_name = .pages[, 1],
    street = .pages[, address_pos],
    city = .pages[, city_pos],
    state = .pages[, state_pos]
  ) |> 
    dplyr::filter(!site_name == "Service Site")
  
  return(out)
}

.data_table_builder_new <- function(.pages) {
  
  ## Get the street address
  address_pos <- stringr::str_which(.pages[1,], "Street Address")
  
  if (length(address_pos) == 0) {
    address_pos <- 3
  }
  
  ## Get the street address
  city_pos <- stringr::str_which(.pages[1,], "City")
  
  if (length(city_pos) == 0) {
    city_pos <- 4
  }
  
  ## Get the state
  state_pos <- stringr::str_which(.pages[1,], "State")
  
  if (length(state_pos) == 0) {
    state_pos <- 5
  }
  
  ## Build the tibble
  out <- tibble::tibble(
    category = .pages[, 1],
    site_name = .pages[, 2],
    street = .pages[, address_pos],
    city = .pages[, city_pos],
    state = .pages[, state_pos]
  )
  
  return(out)
}
