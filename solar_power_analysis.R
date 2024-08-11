library(dplyr)
library(httr)
library(xml2)
library(readr)
library(purrr)
library(zoo)
library(janitor)
library(stringr)
library(tidyr)
library(readxl)
### FUNCTIONS ###
# Function to grab data series from fred
get_fred_data <- function(series_id, api_key) {
  fred_res <- GET(url = paste0(
    "https://api.stlouisfed.org/fred/series/observations?series_id=",
    series_id,
    "&api_key=",
    api_key
  ))
  
  stop_for_status(fred_res)
  
  fred_content <- content(fred_res)
  
  fred_content_series_data <- xml_children(fred_content)
  
  fred_dates <- xml_attr(fred_content_series_data, "date")
  fred_values <- xml_attr(fred_content_series_data, "value")
  
  fred_data <- tibble(
    date = base::as.Date(fred_dates),
    value = fred_values
  )
  
  fred_data_parsed <- fred_data %>% 
    mutate(value = as.double(if_else(value == ".", NA_character_, value))) %>% 
    filter(!is.na(value))
  
  return(fred_data_parsed)
}

# Reading in FRED & EIA api keys
con <- file(description = "~/.api_keys/fred.txt", open = "rt", blocking = F)
FRED_API_KEY <- readLines(con, n = 1)
close(con)
con <- file(description = "~/.api_keys/eia.txt", open = "rt", blocking = F)
EIA_API_KEY <- readLines(con, n = 1)
close(con)

# Getting the unemployment rate from 2007 to present to re-create 
# Brittney Spears 'Work Bitch' graph
unemp_rate <- get_fred_data("UNRATE", FRED_API_KEY)

write_csv(unemp_rate, "./data/raw_data/fred_unrate.csv")

unemp_rate_post_07 <- unemp_rate %>% 
  filter(date >= base::as.Date("2007-05-01")) %>% 
  rename(unemp_rate = value)

write_csv(unemp_rate_post_07, 
          "./data/clean_data/us_unemp_rate_07_24_line_chart_dw.csv")


# Getting solar power generation data from the EIA
eia_res <- GET(
  url = paste0(
    "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/?api_key=",
    EIA_API_KEY,
    "&frequency=monthly&data[0]=generation&facets[fueltypeid][]=TSN&facets[location][]=US&facets[sectorid][]=99&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
  )
)

stop_for_status(eia_res)

eia_content_json <- content(eia_res,
                            as = "parsed",
                            type = "application/json")


eia_solar_capacity <- tibble(
  generation = as.numeric(as_vector(map(eia_content_json$response$data, "generation"))),
  date = as_vector(map(eia_content_json$response$data, "period"))
)

eia_solar_capacity_rolling_12 <- eia_solar_capacity %>% 
  mutate(date = base::as.Date(paste0(date, "-01")),
         generation_rolling_12 = rollmean(generation, k = 12, fill = NA, align = "left")) %>% 
  filter(!is.na(generation_rolling_12))
  

eia_solar_capacity_rolling_12 %>% 
  filter(date >= base::as.Date("2021-06-01")) %>% 
  select(-generation) %>% 
  write_csv("./data/clean_data/us_solar_gen_062021_052024_line_chart_dw.csv")

eia_solar_capacity_rolling_12 %>% 
  select(-generation) %>% 
  write_csv("./data/clean_data/us_solar_gen_122014_052024_line_chart_dw.csv")

# Total electricity generation by major sources
net_elec_gen_types <- read_csv("./data/raw_data/Net_generation_for_all_sectors.csv",
         col_names = T,
         col_types = cols(.default = col_character()),
         skip = 4,
         name_repair = make_clean_names)

net_elec_gen_types_tidy <- net_elec_gen_types %>% 
  filter(!is.na(jan_2014)) %>% 
  select(-c(units, source_key)) %>% 
  mutate(description = str_remove(description, "United States\\s:\\s")) %>% 
  pivot_longer(cols = -description, names_to = "date", values_to = "thousand_megawatthours") %>% 
  mutate(date = base::as.Date(paste0(date, "_01"), format = "%b_%Y_%d"),
         thousand_megawatthours = as.numeric(thousand_megawatthours)) %>% 
  distinct() # removing duplicated "all utility-scale solar" category

# Excluding these duplicate types from the total
excluded_energy_types <- c("wood and wood-derived fuels",
                           "other biomass",
                           "hydro-electric pumped storage", "small-scale solar photovoltaic",
                           "all fuels (utility-scale)")

net_elec_gen_props <- net_elec_gen_types_tidy %>%
  filter(!(description %in% excluded_energy_types)) %>% 
  group_by(date) %>% 
  mutate(prop_total = (thousand_megawatthours / sum(thousand_megawatthours)) * 100) %>% 
  ungroup()
  
major_net_elec_gen_props <- net_elec_gen_props %>% 
  filter(description %in% c("coal", "natural gas", "nuclear", 
                            "conventional hydroelectric",
                            "wind", "all utility-scale solar")) %>% 
  mutate(description = str_to_title(description)) %>% 
  select(-thousand_megawatthours) %>% 
  pivot_wider(names_from = description, values_from = prop_total) %>% 
  rename(Solar = `All Utility-Scale Solar`)

write_csv(major_net_elec_gen_props, 
          "./data/clean_data/us_elec_gen_prop_012014_052024_area_chart_dw.csv")

# Reading in planned electricity generation data
eia_planned_generators0624 <- read_excel(path = "./data/raw_data/june_generator2024.xlsx",
           sheet = "Planned",
           skip = 2,
           col_names = T,
           col_types = "text",
           .name_repair = make_clean_names)

eia_planned_generators_grouped <- eia_planned_generators0624 %>% 
  group_by(planned_operation_year, technology) %>% 
  summarize(nameplate_capacity_gw = sum((as.numeric(nameplate_capacity_mw) / 100))) %>% 
  ungroup() %>% 
  filter(planned_operation_year <= 2028) %>% 
  mutate(technology_group = case_when(
    technology == "Batteries" ~ "Batteries",
    technology == "Solar Photovoltaic" ~ "Solar Photovoltaic",
    T ~ "All Others"
  )) %>% 
  group_by(planned_operation_year, technology_group) %>% 
  summarize(nameplate_capacity_gw = sum(nameplate_capacity_gw)) %>% 
  ungroup() %>% 
  pivot_wider(names_from = planned_operation_year, values_from = nameplate_capacity_gw) %>% 
  mutate(technology_group = factor(technology_group,
                                   levels = c("Solar Photovoltaic", "Batteries", "All Others"),
                                   labels = c("Solar Photovoltaic", "Batteries", "All Others"),
                                   ordered = T)) %>% 
  arrange(technology_group)
  

write_csv(eia_planned_generators_grouped, 
          "./data/clean_data/eia_planned_generators_0624_stacked_column_dw.csv")



