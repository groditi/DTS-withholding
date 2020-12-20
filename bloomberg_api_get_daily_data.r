library(Rblpapi)
library(xts)

bbgconn <- blpConnect()

monthlyOptions <- structure(c("ACTUAL", "DAILY"), names = c("periodicityAdjustment", "periodicitySelection"))
result = bdh("USCBFTWI Index", "PX_LAST", start.date=as.Date("1990-01-01"), con=bbgconn, options=monthlyOptions)
xts_daily_result = xts(result[,2], order.by = as.Date(result[,1]))
names(xts_daily_result) = c("deposit.amount")
write.zoo(xts_daily_result, file = "./etc/withholding-daily-NSA.csv", sep=",")
# 