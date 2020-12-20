library(readr) #read_csv
library(dplyr)
library(zoo) #as.yearmon
library(lubridate) #date stuff
library(purrr) #pmap, map_int
library(tibble)
library(broom) #tidy
library(tidyr)  #pivot_wider

###############################################################################
##    HELPER FUNCTION DECLARATIONS FOR VECTORIZED WEEKDAY IDENTIFICATION     ##
###############################################################################

#max consecutive business days bank can be closed days is 4:
# 1 holiday plus 1 emergency holiday if a (former-)president dies
# if the prev day is 1 or 2 days before isday, and curr.day is after
# isday, consider curr day a makeup.
# *note this is not a valid general and will return false positives in 
# data that has longer gaps!
#internal version that only works 1 call at a time
.is_day <- function(isday, curr.day, prev.day){
  if(isday == curr.day) return(1)  #match
  if(is.na(prev.day) || isday == prev.day) return(0) #no gap
  
  #prev day has be day before yestrerday or the day before that
  if(prev.day != (curr.day+3) %% 5 && prev.day != (curr.day+2) %% 5) return(0)
  if( prev.day == ((isday+4) %% 5) ) return(1) #1day gap
  if( prev.day == ((isday+3) %% 5) ) return(1) #2day gap
  
  return(0)
}

#vectorized version so that dplyr doesnt choke
is_day <- function(isday, curr.day, prev.day){
  if(length(curr.day) != length(prev.day)) return()
  if(length(curr.day) == length(isday)) {
    list(id=as.vector(isday), cd=as.vector(curr.day), pd=as.vector(prev.day) ) %>%
      pmap(.isday(id,cd,pd)) %>% as_vector %>% return
  }
  list(cd=as.vector(curr.day), pd=as.vector(prev.day)) %>%
    pmap(function(cd,pd){.is_day(isday, cd, pd)}) %>% as_vector %>% return
}


#UPDATE: ignore day counts this is done with group_by without sumarise 
# this is stupid inefficient so never use on real code but a one-off hack it works
# for n rows of j periods of k days it does n joins which have an average of 
# ~ (k+1)/2 rows each so for 5 years of 22 day months you would have to do
# 1320 calls to this function with 15,180 rows. the trade off is between function calls
# and using DT/data.tables to do something like one giant JOIN op that could become a memory
# issue in very large data sets. but honestly the efficient way to do this that is very un-R-like
# would be to simply oull the index() from daily_actuals and iterate through it in a loop
# to create a matching vector of integers and then merge that in to the tibble

#returns the an index (1 based) representing the nth positon of today in the month
.day.count <- function(dataset, date){
  dataset %>% filter( Index >= floor_date(date,"month"), Index <= date) %>% tally() %>% as.integer()
}

#vectorized version
day_count <- function(dataset, date){
  date %>% map_as.integer(~.day.count(dataset, .x)) 
}

###############################################################################
# IMPORT AND TRANSFORM
###############################################################################

#read the raw dailies
daily_actuals <- read_csv(file.path("etc", "withholding-daily-NSA.csv"))
  

#read the raw monthlies
monthly_data <- read_csv(file.path("etc", "withholding-monthly-NSA.csv")) %>%
  mutate(
    year.month = as.yearmon(date), 
    day.average = deposit.amount / day.count,
    prev.day.count = lag(day.count),
    month.length = day(ceiling_date(date,"months")-days(1)),
    prev.month.length = lag(month.length)
  )


#transform the dailies to populate the independent variables
 #int calls are for integer coercion
daily_model_data <- daily_actuals %>% rename(date = Index) %>% 
  group_by( year(date), month(date) ) %>% 
  mutate( business.day.of.month = cumsum(date>0) )  %>%
  ungroup() %>% select(-ends_with("(date)")) %>% mutate(
    #year-month for joining with monthly
    year.month = as.yearmon(date),

    #0-4 weekday indicator (well, to 6, but weekends should never show)
    weekday = ((wday(date))+5) %% 7,   
    
    #this was stupid and i solved it better
    #business.day.of.month = day_count(daily_actuals, date),
    
    #lookbacks and look-aheads
    prev.weekday = lag(weekday),
    prev.date = lag(date),
    next.date = lead(date),
    
    is.wednesday.semiweekly = as.integer(
      (weekday == 2 & lag(weekday, 2) == 0 & lag(weekday, 1) == 1) | #normal
      (weekday == 3 & ( #thursday where any of prev mon tues or weds are were off
        (lag(weekday, 3) != 0 | lag(weekday, 2) != 1) | lag(weekday, 1) != 2)
      )
    ),
    is.friday.semiweekly = as.integer(
      (weekday == 4 & lag(weekday, 2) == 2 & lag(weekday, 1) == 3) | #normal
      (weekday == 0 & ( #monday where any of prev fri thurs weds were off
        (lag(weekday, 3) != 2 | lag(weekday, 2) != 3) | lag(weekday, 1) != 4)
      )
    ),
 
    
    #month and year start flags for calendar-tern pattern
    is.month.start = as.integer(month(prev.date) != month(date)),
    is.month.end = as.integer(month(next.date) != month(date)),
    is.year.start = as.integer(year(prev.date) < year(date)),
    is.year.end = as.integer(year(next.date) > year(date)),
    
    #day of week predicates for monday/wednesday/friday patterns
    is.monday = as.integer(is_day(0, weekday, prev.weekday)),
    is.tuesday = as.integer(is_day(1, weekday, prev.weekday)),
    is.wednesday = as.integer(is_day(2, weekday, prev.weekday)),
    is.thursday = as.integer(is_day(3, weekday, prev.weekday)),
    is.friday = as.integer(is_day(4, weekday, prev.weekday)),
    
    #monthly predicates for monthly patterns
    # sixteen means calendar day 16 or the next business day
    is.fifteen = as.integer(day(prev.date) < 15 & day(next.date) > 15 & day(date) >= 15 ),
    is.sixteen = as.integer(day(prev.date) < 16 & day(next.date) > 16 & day(date) >= 16 ),

    #payroll calendar for the second and 11th business day of the month
    is.second = as.integer(business.day.of.month == 2),
    is.eleventh = as.integer(business.day.of.month == 11),
    is.third = as.integer(business.day.of.month == 3),
    is.twelveth = as.integer(business.day.of.month == 12),
    
    #special patterns based on holidays or tax deadlines
    is.xmas = as.integer(month(date) == 12 & day(date) < 25 & day(next.date) > 25),
    is.post.thanksgiving = as.integer(is.month.start & month(date) == 12),
  
    #march has a seasonal effect, in april, the sixteen pattern moves to 17/18
    # because of tax day
    is.march.sixteen = as.integer(is.sixteen & month(date) == 3),
    is.april.sixteen = as.integer(is.sixteen & month(date) == 4),
    is.tax.season = as.integer(month(date) == 4 & day(date) > 16 & day(date) < 19),

        
    #find the first and third calendar monday calendar dates
    third.monday.date = days(((9-wday(floor_date(date,"months"))) %% 7) + 14)+floor_date(date,"months"),
    first.monday.date = third.monday.date - days(14),
  
    #find the first semiweekly deposit. note if_else so the date isnt coerced into an integer
    first.wednesday.date = days(((11-wday(floor_date(date,"months"))) %% 7) )+floor_date(date,"months"),
    first.friday.date = days(((13-wday(floor_date(date,"months"))) %% 7) )+floor_date(date,"months"),
    first.semiweekly.date = if_else(first.friday.date > first.wednesday.date,first.wednesday.date,first.friday.date),
    
    # these mondays are just special. i think this might be the federal payroll pattern tbh
    #bimonthly monday predicates.. if you change the & to && the code breaks tidyeval lol
    is.third.monday = as.integer(
      third.monday.date > prev.date & third.monday.date < next.date & date >= third.monday.date
    ),
    is.first.monday = as.integer(
      first.monday.date > prev.date & first.monday.date < next.date & date >= first.monday.date
    ),
    is.first.wednesday = as.integer(
      first.wednesday.date > prev.date & first.wednesday.date < next.date & date >= first.wednesday.date
    ),
    is.first.friday = as.integer(
      first.friday.date > prev.date & first.friday.date < next.date & date >= first.friday.date
    ),
    is.first.semiweekly = as.integer(
      business.day.of.month <= 3 & (is.friday.semiweekly | is.wednesday.semiweekly)
    ),

##originally wanted to try this as a cyclical flag for longer multi-year cycles but it wasnt right 
#     month.starts.sunday = as.integer(wday(floor_date(date,"months")) == 0),
#     month.starts.monday = as.integer(wday(floor_date(date,"months")) == 1),
#     month.starts.tuesday = as.integer(wday(floor_date(date,"months")) == 2),
#     month.starts.wednesday = as.integer(wday(floor_date(date,"months")) == 3),
#     month.starts.thursday = as.integer(wday(floor_date(date,"months")) == 4),
#     month.starts.friday = as.integer(wday(floor_date(date,"months")) == 5),
#     
        
    #TODO: I was going to add an alternating monday flag for strictly biweekly payrolls and maybe
    #test it with one that re-sets every year but uuuugh this is ~fine~ and like, who knows if it
    #would even be significant. it's already a lot of variables for not a ton of training data
  ) %>% inner_join(
    select(
      monthly_data, year.month, day.average, day.count, month.length, month.deposit.amount = deposit.amount
    ), by = "year.month" 
  )

# train the model on Dec 2013 to Dec 2019 only
# because the deposits have a natural drift, de-trend them by converting them into
# a percent of the daily average for the month. We don't use a percent of the month
# total because this makes it easier to deal with unstandard month lengths
# a finaladjustment for the raio of calendar days to business days finishes that up 
pre_covid <- daily_model_data %>% 
  filter( date < ymd('2020-01-01') & date >= ymd('2013-02-01') ) %>% 
  mutate(
    pct.deposit = deposit.amount / day.average,
#    month.stage =  as.integer(business.day.of.month > 21),
    business.day.pct = (day.count / month.length),
    days.to.end = day.count - business.day.of.month, 
    # is.fifth.week = as.integer(business.day.of.month > 20 & day.count >= 22),
  )


# is.eleventh +  is.third.monday + is.first.wednesday +
#compute the model
daily_patterns = lm(
  pct.deposit ~ is.monday + is.wednesday + is.thursday + is.friday + is.fifteen +
      is.sixteen +  is.second + is.third + is.month.start + is.month.end + 
      is.year.start + is.year.end + is.march.sixteen + is.april.sixteen +
      is.tax.season  + is.post.thanksgiving + is.xmas + business.day.pct +
      is.first.wednesday + is.twelveth +
      is.wednesday.semiweekly + is.friday.semiweekly,
   data = pre_covid
)



# merge the model_data with the fitted values and residuals
# and add a new columns for diagnostics
check_data <- pre_covid %>% add_column(
  fitted.value = daily_patterns$fitted.values,
  residual = daily_patterns$residuals
) %>% mutate ( 
    predicted.mean = deposit.amount / fitted.value,
    day = day(date),
    month = month(date),
    year = year(date)
  ) %>% group_by(year.month) %>% mutate(
    mtd.fitted = cumsum(fitted.value),
    mtd.actual = cumsum(pct.deposit),
    mtd.residual = mtd.actual - mtd.fitted,
    mtd.deposit = cumsum(deposit.amount),
    mtd.month.est = (mtd.deposit / mtd.fitted) * day.count,
    mtd.error = (month.deposit.amount / mtd.month.est) - 1,
    month.fitted = sum(fitted.value),
    scale.factor = day.count / month.fitted,
    
    #this was bad bad bad and dumb dumb dumb    
    # mtd.predicted.sa = cumsum(predicted.mean),
    # mtd.month.predicted.sa = mtd.predicted.sa / (business.day.of.month / day.count),
    # mtd.precision = (mtd.month.predicted.sa / month.deposit.amount) - 1
  ) %>% ungroup()


#turn the coefficients into a table
#will use this to generate a function to generate an estimator function

coeff_table <- tidy(daily_patterns)

coeff_table %>% write_csv(file.path("etc", "daily-model-coefficients.csv"))

# save the inputs and outputs of the model
check_data %>% write_csv(file.path("etc", "daily-model-data.csv"))

daily_model_data %>% write_csv(file.path("etc", "daily-all-data.csv"))

check_data %>% group_by(year = year(date), month = month(date)) %>% 
  summarise( residuals = mean(residual) ) %>%
  pivot_wider( names_from = year, values_from = residuals ) %>% arrange(month) %>%
  write_csv(file.path("etc", "daily-diagnostics-by-year-month.csv"))

check_data %>% group_by(month = month(date), day = day(date)) %>% 
  summarise( residuals = mean(residual) ) %>%
  pivot_wider( names_from = month, values_from = residuals ) %>% arrange(day) %>%
  write_csv(file.path("etc", "daily-diagnostics-by-month-day.csv"))

check_data %>% group_by(month = month(date), day = business.day.of.month) %>% 
  summarise( residuals = mean(residual) ) %>%
  pivot_wider( names_from = month, values_from = residuals ) %>% arrange(day) %>%
  write_csv(file.path("etc", "daily-diagnostics-by-month-business-day.csv"))

check_data %>% group_by(month = month(date)) %>% 
  summarise( residuals = mean(residual) ) %>%
  arrange(month) %>%
  write_csv(file.path("etc", "daily-diagnostics-by-month.csv"))

check_data %>% group_by(day = day(date)) %>% 
  summarise( residuals = mean(residual) ) %>%
  arrange(day) %>%
  write_csv(file.path("etc", "daily-diagnostics-by-day.csv"))


check_data %>% group_by(day = business.day.of.month) %>% 
  summarise( residuals = mean(residual) ) %>%
  arrange(day) %>%
  write_csv(file.path("etc", "daily-diagnostics-by-business.day.csv"))

check_data %>% select(year.month, days.to.end, mtd.error) %>% arrange(days.to.end) %>%
  pivot_wider( names_from = days.to.end, values_from = mtd.error ) %>% arrange(year.month) %>%
  write_csv(file.path("etc", "daily-diagnostics-est-error-by-days-to-eom.csv"))


check_data %>% select(mtd.error, business.day.of.month) %>% 
  group_by(business.day.of.month) %>% summarize(
    mean.err = mean(mtd.error),
    sd.err = sd(mtd.error),
    median.err = median(mtd.error),
    mad.err = mad(mtd.error)
  ) %>% write_csv(file.path("etc", "daily-diagnostics-cum-error-by-business-day.csv"))


#
#VISUALIZE HOW THE ESTIMATE DEVELOPS OVER TIME

# error_plot_data <- check_data %>% select(mtd.precision, business.day.of.month)
# error_plot_aggs <- error_plot_data %>% group_by(business.day.of.month) %>%
#   summarize(
#     mean.est = mean(mtd.precision),
#     sd.est = sd(mtd.precision),
#     sd.1lower = mean.est - sd.est,
#     sd.1higher = mean.est + sd.est,
#     sd.2lower = mean.est - 2*sd.est,
#     sd.2higher = mean.est + 2*sd.est,
#     median.est = median(mtd.precision),
#     mad.est = mad(mtd.precision)
#   )
#
# ggplot() + geom_point(
#   data = error_plot_data,
#   mapping = aes(x = business.day.of.month, y=mtd.precision),
#   alpha = 0.2,
#   color = "royalblue3"
#   ) + geom_ribbon(
#     data = error_plot_aggs,
#     mapping = aes(
#       x = business.day.of.month,
#       ymin = sd.2lower,
#       ymax = sd.2higher,
#     ),
#     color = "royalblue3",
#     alpha = 0.15,
#     fill = "royalblue2"
#   ) + geom_ribbon(
#     data = error_plot_aggs,
#     mapping = aes(
#       x = business.day.of.month,
#       ymin = sd.1lower,
#       ymax = sd.1higher,
#     ),
#     color = "royalblue3",
#     alpha = 0.15,
#     fill = "royalblue2"
#   ) + geom_line(
#     data = error_plot_aggs,
#     mapping = aes(
#       x = business.day.of.month,
#       y = mean.est
#     ),
#     color = "royalblue4",
#     alpha = 0.6
#   )
#
