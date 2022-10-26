#----------------------Title X Clinic Location Scraping-------------------------
#-Author: A. Jordan Nafa---------------------------Created: September 25, 2022-#
#-R Version: 4.2.1-----------------------------Last Modified: October 26, 2022-#

## Load the required packages
pacman::p_load(
  tidyverse,
  arrow,
  tabulizer,
  rvest,
  furrr,
  googleway,
  install = FALSE
)

# Set Session Options
options(
  digits = 6, # Significant figures output
  scipen = 999, # Disable scientific notation
  repos = getOption("repos")["CRAN"],
  knitr.kable.NA = '',
  dplyr.summarise.inform = FALSE
)

# Source the helper functions
source("scripts/pdf-scraping-helpers.R")

#------------------------------------------------------------------------------#
#-----------------------Retrieving the File Locations---------------------------
#------------------------------------------------------------------------------#

## Scrape the URLS for the PDF files
web_links <- read_html("https://opa.hhs.gov/grant-programs/archive/title-x-program-archive/title-x-directory-archive") %>% 
  html_elements("div ul li") %>% 
  .[284:354]

## Pull the url links
file_links <- map_chr(
  .x = seq_along(web_links),
  ~ str_extract(
    as.character(web_links[[.x]]), 
    pattern = "sites.*.pdf"
  )
)

## Append the file extension to the base link
pdf_links_df <- tibble(
  base_link = file_links,
  file_links =  str_c("https://opa.hhs.gov/", file_links),
  month = str_extract(
    string = base_link, 
    pattern = regex(
      paste0(month.name, collapse = "|"), 
      ignore_case = TRUE
      )
    ) %>% str_to_title(),
  year = str_extract(
    string = str_remove_all(base_link, pattern = "sites.*(t|T)"), 
    pattern = paste0(2015:2022, collapse = "|")
  )
)

#------------------------------------------------------------------------------#
#---------------------Scraping the Tables for Each Month------------------------
#------------------------------------------------------------------------------#

## Initialize parallel scraping
plan(multisession(workers = 4L))

## Scrape the file links and split the PDF into structured pages
pdf_links_df <- pdf_links_df %>% 
  mutate(pdf_tables = future_map(
    file_links,
    ~ extract_tables(file =.x)
  )) %>% 
  # Unnest the data tables
  unnest(cols = pdf_tables) %>% 
  mutate(
    # Obtain the number of columns in the data matrices
    ncols = map_dbl(pdf_tables, ~ ncol(.x)),
    # Fix a problem with the date regex
    year = case_when(
      str_detect(base_link, "June%202022_508") ~ "2022",
      TRUE ~ year
    ) %>% as.integer()
    ) %>% 
  # Exclude pages with just headers
  filter(ncols > 3)

# Back from the future
plan(sequential)

## Write the result to a file so we don't have to run this again
write_rds(pdf_links_df, "data/title_x_pdf_scraped.rds")

#------------------------------------------------------------------------------#
#-------------Converting the Scraped Text tables into Data Frames---------------
#------------------------------------------------------------------------------#

## Write the result to a file so we don't have to run this again
pdf_links_df <- read_rds("data/title_x_pdf_scraped.rds") 

# Construct tibbles for each scraped page
title_x_pdf_scraped <- pdf_links_df %>% 
  mutate(pdf_tables_df = map(
    .x = 1:nrow(.),
    ~ table_builders(
      .pages = pdf_links_df$pdf_tables[[.x]], 
      .month = pdf_links_df$month[.x],
      .year = pdf_links_df$year[.x]
    )
  ))

# Split the PDF tables
title_x_pdf_scraped <- title_x_pdf_scraped[-6284, ] %>% 
  # Preparing the tibbles
  mutate(pdf_tables_preped = map2(
    .x = pdf_tables_df,
    .y = str_c(year, month, sep = "-"),
    ~ address_cleaning(.x, .y)
    ))

## Write the result to a file so we don't have to run this again
write_rds(title_x_pdf_scraped, "data/title_x_pdf_scraped_cleaned.rds")

# Combine this into a single data frame
title_x_df <- bind_rows(
  title_x_pdf_scraped$pdf_tables_preped,
  .id = "row_id"
  ) %>% 
  # Filter out row headers
  filter(!state == "State" & nchar(state) == 2) %>% 
  # Set any "" cells to NA
  mutate(across(street:site_name_b, ~ na_if(.x, "")))

#------------------------------------------------------------------------------#
#-------------------Geocoding Using the Google Places API-----------------------
#------------------------------------------------------------------------------#

# Note: This requires a google cloud account. For more information on the 
# places API, see https://developers.google.com/maps/documentation/places/

title_x_df_sub <- title_x_df %>% 
  # Use unique addresses for geo-coding
  distinct(street, city, state) %>% 
  # Sort the data by state and city
  arrange(state, city) %>% 
  # Build the API calls
  mutate(api_call = str_c(street, city, state, sep = ", "))

# Initialize a list to store things in
clinic_locations_fixed <- list()

# Define the API key
api_key = "YOU_API_KEY_HERE"

# Looping over each address and calling the google places api
for (j in 1:nrow(title_x_df_sub)) {
  clinic_locations_fixed[[j]] <- google_find_place(
    input = title_x_df_sub$api_call[j],
    key = api_key,
    inputtype = "textquery",
    fields = c("formatted_address","geometry",
               "id","name","permanently_closed",
               "place_id","types")
  )
}

## Write this to a file
write_rds(clinic_locations_fixed, "data/clinic-places-final.rds")

# Map the location objects to the original data frame
title_x_df_geocodes <- title_x_df_sub %>% 
  mutate(locations = map(
    .x = 1:nrow(title_x_df_sub),
    ~ as_tibble(clinic_locations_fixed[[.x]]$candidates)
    )) %>% 
  # Unnest the data
  unnest(cols = locations)

#------------------------------------------------------------------------------#
#-----------------------Building the Final Data Files---------------------------
#------------------------------------------------------------------------------#

# Merge in the geo-coding information
title_x_full <- title_x_df %>% 
  left_join(
    title_x_df_geocodes, 
    by = c("city", "state", "street"),
    na_matches = "never"
  ) %>% 
  # Keep distinct locations
  distinct(formatted_address, date, .keep_all = TRUE)

# Write the uncleaned but geocoded data to an rds file
write_rds(
  title_x_full,
  "data/title-x-locations-raw.rds", 
  compress = "gz", 
  compression = 9L
  )

title_x_final <- title_x_full %>% 
  # Exclude irrelevant site names
  filter(!str_detect(
    site_name_a, 
    "(s|S)chool|(C|c)orrectional|(p|P)rison|Department of Health"
  )) %>% 
  # Transmute a subset of the data
  transmute(
    # Date
    date = date,
    # Address based on google geocoding
    location = formatted_address,
    # Name of the place based on title X directories
    place_name = case_when(
      is.na(site_name_b) ~ site_name_a,
      site_name_a == site_name_b ~ site_name_a,
      site_name_b != site_name_a ~ site_name_b
    ),
    # Latitude
    lat = geometry$location$lat,
    # Longitude
    lon = geometry$location$lng,
    # State Abbreviation
    state = state,
    # Google Place ID
    google_id = place_id,
    # Name in Google Places
    place = name
  ) %>% 
  # Split the date back into month and year
  separate(col = date, into = c("year", "month"), sep = "-") %>% 
  # Sort the data by location, year, and month
  arrange(state, location, year, month) 

# Write the cleaned data to an parquet file
write_parquet(
  title_x_final,
  "data/title-x-locations-monthly.gz.parquet", 
  compression = "gzip", 
  compression_level = 6L
)

# Annual version of the data
title_x_yearly <- title_x_final %>% 
  # Keep distinct observations by year
  distinct(location, year, .keep_all = TRUE) %>% 
  # Drop month column
  select(-month) %>% 
  # Sort by state, location, and year
  arrange(state, location, year)
  
# Write the cleaned yearly data to an parquet file
write_parquet(
  title_x_yearly,
  "data/title-x-locations-yearly.gz.parquet", 
  compression = "gzip", 
  compression_level = 6L
)
