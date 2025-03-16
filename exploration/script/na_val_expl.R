library(tidyverse)
library(VIM)

df = readr::read_csv("train_with_feats.csv")
df = df |> distinct()

# drop row with all na value
features_name = df |> dplyr::select(-c(ticker, date_q, price)) %>% colnames
df = df |> filter((!if_all(features_name, is.na)))

#1) check na presence
na_val = function(data){
  n_na = sapply(data, function(col) sum(is.na(col)))
  n_na = sort(n_na[n_na > 0])
  na_data = data.frame(
    variabile = names(n_na),
    freq_assoluta = as.numeric(n_na),
    freq_relativa = round(as.numeric(n_na)/nrow(data), 6)
  )
  return(na_data) #return: name, freq abs. freq. rel. of missing
}
na_freq = na_val(df)
na_freq

# delete all feats with more than 30% null
feat_to_del = na_freq |> filter(freq_relativa > 0.3) |> dplyr::select(variabile)
df = df |> dplyr::select(-all_of(feat_to_del[["variabile"]]))

# set other to null value in finnhubindustry
df$finnhubIndustry = ifelse(is.na(df$finnhubIndustry), 'other', df$finnhubIndustry)

# 
marginplot(df[,c('totalRatio', 'totalDebtToTotalAsset')])
marginplot(df[,c('ebitPerShare', 'roe')])


df |> 
  filter(price < 100) |>
  ggplot(aes(y = price, x = is.na(ebitPerShare))) +
  geom_boxplot()

df |> 
  filter(price < 100) |>
  ggplot(aes(y = price, x = is.na(fcfPerShare))) +
  geom_boxplot() 


# test the independence of na presence in different feats
prop.table(table(is.na(df["ebitPerShare"]), is.na(df["fcfPerShare"])), 2)
tab_ebit_roe = table(is.na(df["ebitPerShare"]), is.na(df["fcfPerShare"]))
chisq.test(tab_ebit_roe)
# the presence of na in on feats depend on the presence in another feats
# missing not a randoom

df |> 
  filter(fcfPerShare < 100 & fcfPerShare > -100) |>
  ggplot(aes(x = fcfPerShare, group = is.na(ebitPerShare), color = is.na(ebitPerShare))) +
  geom_density() 






