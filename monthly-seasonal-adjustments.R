library(seasonal)
library(readr)
library(dplyr)
library(zoo)
library(lubridate)


#extract the forecasts from a seas object
extract_forecasts <- function(sa_monthly_model) { 
  forecasts_ts <- sa_monthly_model %>% series('fct')
  sa_forecasts <- tibble(
    date = as.Date(as.yearmon(index(forecasts_ts))),
    forecast = forecasts_ts[,1],
    lower.ci = forecasts_ts[,2],
    upper.ci = forecasts_ts[,3]
  )
}

#extract the estimates form a seas object
extract_estimates <- function(sa_monthly_model) { 
  sa_estimates <- tibble(
    date = as.Date(as.yearmon(index(series(sa_monthly_model, 'seats.seasonal')))),
    final = final(sa_monthly_model),
    seasonal = series(sa_monthly_model, 'seats.seasonal'),
    seasonal.adjustment = series(sa_monthly_model, 'seats.seasonaladj'),
    trend = series(sa_monthly_model, 'seats.trend'),
    irregular = series(sa_monthly_model, 'seats.irregular'),
    adjustment.factor = series(sa_monthly_model, 'seats.adjustfac'),
    adjustment.ratio = series(sa_monthly_model, 'seats.adjustmentratio'),
    residual = series(sa_monthly_model,'estimate.residuals')
  )
}


#read the raw dailies
daily_actuals <- read_csv(file.path("etc", "withholding-daily-NSA.csv")) %>%
  rename(date = Index)

#aggregate into monthlies
monthly_nsa <- daily_actuals %>% 
  mutate(year.month = as.yearmon(date)) %>%
    group_by(year.month) %>%
      summarise( deposit.amount = sum(deposit.amount), day.count = n())


#generate a ts object for the seasonal package
start_date <- c(2006,1)
end_date <- as.numeric(as.yearmon(today())) #month before current
monthly_ts <- monthly_nsa %>% filter(year.month < end_date  & year.month >= 2006) %>% 
  select(deposit.amount) %>% ts(frequency = 12, start = start_date)

pre_covid_monthly_ts <- monthly_nsa %>% filter(year.month < 2020 & year.month >= 2006) %>% 
  select(deposit.amount) %>% ts(frequency = 12, start = start_date)

all_monthly_ts <- monthly_nsa %>% filter(year.month < end_date  & year.month >= 1990) %>% 
  select(deposit.amount) %>% ts(frequency = 12, start = c(1990,1))

#generate a SEATS model for seasonality adjustment + forecasts
sa_monthly_model <- monthly_ts %>% seas( forecast.save = "fct" )
pre_covid_sa_monthly_model <- pre_covid_monthly_ts %>% seas( forecast.save = "fct" )
all_sa_monthly_model <- all_monthly_ts %>% seas( forecast.save = "fct" )

#save the monthly model and the forecasts
monthly_nsa %>% mutate( date = as.Date(year.month) ) %>% 
  select(date, deposit.amount, day.count) %>% write_csv(file.path("etc", "withholding-monthly-NSA.csv"))

#save the currentmonthly model and the forecasts
extract_estimates(sa_monthly_model) %>%
  write_csv(file.path("etc", "withholding-monthly-SA.csv"))
extract_forecasts(sa_monthly_model) %>%
  write_csv(file.path("etc", "withholding-monthly-forecasts.csv"))

#save the pre-covid monthly model and the forecasts
extract_estimates(pre_covid_sa_monthly_model) %>%
  write_csv(file.path("etc", "pre-covid-withholding-monthly-SA.csv"))
extract_forecasts(pre_covid_sa_monthly_model) %>%
  write_csv(file.path("etc", "pre-covid-withholding-monthly-forecasts.csv"))

tibble(
  date = as.Date(as.yearmon(index(series(all_sa_monthly_model, 'seats.seasonal')))),
  final = final(all_sa_monthly_model),
  seasonal = series(all_sa_monthly_model, 'seats.seasonal'),
  seasonal.adjustment = series(all_sa_monthly_model, 'seats.seasonaladj'),
  trend = series(all_sa_monthly_model, 'seats.trend'),
  irregular = series(all_sa_monthly_model, 'seats.irregular'),
  adjustment.factor = series(all_sa_monthly_model, 'seats.adjustfac')
) %>% write_csv(file.path("etc", "all-withholding-monthly-SA.csv"))
