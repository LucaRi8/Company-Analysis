library(tidyverse)

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
