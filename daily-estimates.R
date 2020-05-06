library(readr) #read_csv
library(dplyr)
library(zoo)
library(lubridate) #date stuff
library(tibble)


coeff_table <- read_csv(file.path("etc", "daily-model-coefficients.csv"))

seats_forecasts <- read_csv(file.path("etc", "withholding-monthly-forecasts.csv")) %>%
  mutate(year.month = as.yearmon(date))

daily_model_data <- read_csv(file.path("etc", "daily-all-data.csv")) %>%
  select(-year.month) %>% mutate(year.month = as.yearmon(date))

y_intercept <- coeff_table$estimate[1]
term_names <- coeff_table$term[-1]
coefficient_values <- coeff_table$estimate[-1]

#april isnt done yet so back-fill it for now
post_covid <- daily_model_data %>% filter(year.month >= 2020) %>%
  mutate(
    day.count = ifelse(year.month == 2020.333, 20, day.count),
    business.day.pct = day.count / month.length,
  ) %>% replace_na(
    as.list(sapply(term_names, function(.) return(0))) #i know, i know. really, i do. 
  ) %>% inner_join(
    select(seats_forecasts, -date), by = 'year.month'
    )

#pre_covid <- daily_model_data %>% filter( date < ymd('2020-01-01') & date > ymd('2013-02-01') ) %>%
#  mutate(
#    business.day.pct =  day.count / month.length,
#  ) %>% replace_na(
#    as.list(sapply(term_names, function(.) return(0))) #i know, i know. really, i do. 
#  )



#Select the variables out of the data, turn then into a matrix and transpose it
terms_matrix <- post_covid %>% select(all_of(term_names)) %>% as.matrix() %>% t()
contributor_values = as_tibble(t(terms_matrix * coefficient_values))

#rename columns with the model variable contributions by preceding them with "value."
new_col_names <- sapply(
  term_names, function(name) paste("value",name, sep =".")
) %>% as.vector()
names(contributor_values) = new_col_names

#put it all back together again
post_covid_final <- post_covid %>% add_column(
  value.y_intercept = y_intercept,
  contributor_values
)  %>% mutate(
  fitted.value = rowSums(select(., all_of(c("value.y_intercept",new_col_names)))),
  predicted.mean = deposit.amount / fitted.value,
  day = day(date),
  month = month(date),
  year = year(date),
  days.to.end = day.count - business.day.of.month,
  ) %>% group_by(year.month) %>% mutate(
  mtd.fitted = cumsum(fitted.value),
  mtd.deposit = cumsum(deposit.amount),
  mtd.month.est = (mtd.deposit / mtd.fitted) * day.count,
  mtd.error = (forecast / mtd.month.est) - 1,
  month.fitted = sum(fitted.value),
  scale.factor = day.count / month.fitted,
) %>% ungroup()


#post_covid_final %>% group_by(year = year(date), month = month(date)) %>% 
#  summarise( fitted.values = sum(fitted.value) ) %>%
#  pivot_wider( names_from = year, values_from = fitted.values ) %>% arrange(month)

post_covid_final %>% write_csv(file.path("etc", "daily-predicted-data.csv"))





#code to generate the evolving SA estimate
# library(seasonal)
# library(purrr)
# library(xts)
# 
# #pull the april estimates
# april_2020 <- post_covid_final %>% filter( year == 2020, month == 4) %>%
#   mutate(mtd.month.est.scaled = mtd.month.est * scale.factor) %>%
#   pull(mtd.month.est.scaled) 
# 
# start_date <- c(2006,1)
# training_data <- daily_model_data %>% 
#   filter(year.month < 2020 & year.month >= 2006) %>% 
#   group_by(year.month) %>% summarise(deposit.amount = sum(deposit.amount)) %>%
#   pull(deposit.amount)
# 
# new_data <- daily_model_data %>% 
#   filter(year.month < 2020.333 & year.month >= 2006) %>% 
#   group_by(year.month) %>% summarise(deposit.amount = sum(deposit.amount)) %>%
#   pull(deposit.amount) 

#this was for an attempt to use a model and then update it with data for a forecast
#of the full SA using the old model but it turns out you can't do it unless you
#have 36 months
 # training_ts <- ts(c(training_data ), frequency = 12, start = start_date)
 # trained_model <- seas(training_ts, forecast.save = "fct" )
 # trained_final <- final(trained_model)
 # forecasts_ts <- trained_model %>% series('fct')
 # as.xts(forecasts_ts)["2020-04-01"]
 # sa_forecasts <- tibble(
#   date = as.Date(as.yearmon(index(forecasts_ts))),
#   forecast = forecasts_ts[,1],
#   lower.ci = forecasts_ts[,2],
#   upper.ci = forecasts_ts[,3]
# )
# 222915 255581 

# update_model <- function(...){
#   new_ts <- ts(c(...), frequency = 12, start = c(2006,1))
#   # not using update.seas() because that needs at least 3 years of data
#   new_model <- seas(new_ts, forecast.save = "fct" )
#   list(
#     input = new_ts,
#     final = final(new_model),
#     details = new_model$data,
#     forecasts = series(new_model,'fct')
#   ) %>% return()
# }
# 
# #this is going to take a while
# scenarios = map(april_2020,function(april) update_model(new_data,april))

#22 days in april so let's do 2, 7 ,12, 17, 22
#UPDATE: Turns out the data wont actually change the forecast at all or
#mess with the seasonal adjustment so all we need to do is pull a single
#datum per scenario, but leaving this in here just in case i want to use
#it later
# pull_seats_scenario <- function(i){
#   fc_xts <- as.xts(scenarios[[i]]$forecasts)$forecast
#   fn_xts <- as.xts(scenarios[[i]]$final)
#   idx <- c(index(fn_xts), index(fc_xts)) %>%
#     as.yearmon() %>% as.Date()
#   return(xts(x=c(as.vector(fn_xts), as.vector(fc_xts)), order.by = idx))
# }
#this wasn't really working for viz purposes
# progression <- merge.xts(
#   pull_seats_scenario(2),
#   pull_seats_scenario(7),
#   pull_seats_scenario(12),
#   pull_seats_scenario(17),
#   pull_seats_scenario(22)
# )
# as.vector(progression["2020-04-01"])

# progression <- sapply( c(1:22), function(i) as.vector(pull_seats_scenario(i)["2020-04-01"]) )
# tibble(
#   date = post_covid_final %>% filter( year == 2020, month == 4) %>% pull(date),
#   forecast = progression
# ) %>% write_csv(file.path("etc", "april-daily-projections.csv"))
