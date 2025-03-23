library(tidyverse)
library(VIM)
library(mice)

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

# use t-1 price to control for autocorreelated data
df = df |>
  left_join(
    df |> 
      dplyr::select(ticker, date_q, price) |>
      mutate(date_q = as.Date(date_q)  %m+% months(3)) |>
      rename(c('price_tm1' = price)),
    join_by(ticker, date_q)
  )


imputed_df = kNN(df, 
                  df %>% dplyr::select(-c(ticker, date_q, price)) %>% colnames, 
                  k=15)

colnames(imputed_df)
summary(lm(log(price) ~ log(price_tm1) + . - price_tm1, 
             data = imputed_df[, 1:53] |> dplyr::select(-c(ticker, date_q))))


# check the important feats for prevision purpose
library(glmnet)
lambda = exp(seq(-10,10, length=300))
id_df = data.frame(
  cbind(imputed_df[, 'ticker'] %>%  unique, 
        sample(1:5, length(imputed_df[, 'ticker'] %>%  unique), replace = T))
)
colnames(id_df) = c('ticker', 'id')
id_df = imputed_df['ticker'] |> 
  left_join(
    id_df,
    join_by(ticker)
)
X_imp = imputed_df[, 1:53] |> 
  dplyr::select(-c(ticker, date_q, price, price_tm1))

model_cv = cv.glmnet(x = model.matrix(~ -1 + ., X_imp),
                  y = log(imputed_df[['price']]), 
                  alpha = 0, lambda = lambda, 
                  standardize = TRUE,
                  intercept = TRUE)
plot(model_cv)
model_cv$lambda.min
model_cv$cvm %>%  min
model = glmnet(x = model.matrix(~ -1 + ., X_imp),
               y = log(imputed_df[['price']]), 
               alpha = 0, lambda = model_cv$lambda.min,
               standardize = TRUE,
               intercept = TRUE,)
model$beta

# adaptive lasso model
lasso_model_cv = cv.glmnet(x = model.matrix(~ -1 + ., X_imp),
                     y = log(imputed_df[['price']]), 
                     alpha = 1, lambda = lambda, 
                     standardize = TRUE,
                     intercept = TRUE,
                     penalty.factor = 1/abs(model$beta))
plot(lasso_model_cv)
lasso_model_cv$lambda.min
lasso_model_cv$cvm %>%  min
lasso_model_imputed = glmnet(x = model.matrix(~ -1 + ., X_imp),
               y = log(imputed_df[['price']]), 
               alpha = 1, lambda = lasso_model_cv$lambda.min,
               standardize = TRUE,
               intercept = TRUE,
               penalty.factor = 1/abs(model$beta))
lasso_model_imputed$beta


# use original data and drop NA rows
lambda = exp(seq(-10,5, length=100))
df_nona = df %>%  na.omit
# 5808 rows not na and 669 different company
id_df = data.frame(
  cbind(df_nona[, 'ticker'] %>%  unique, 
        sample(1:5, length(df_nona[, 'ticker'] %>%  unique), replace = T))
)
colnames(id_df) = c('ticker', 'id')
id_df = df_nona['ticker'] |> 
  left_join(
    id_df,
    join_by(ticker)
  )
X = df_nona |> 
  dplyr::select(-c(ticker, date_q, price))

model_cv = cv.glmnet(x = model.matrix(~ -1 + ., data = X),
                     y = log(df_nona[['price']]), 
                     alpha = 0, lambda = lambda, 
                     standardize = TRUE,
                     intercept = TRUE)
plot(model_cv)
model_cv$lambda.min
model_cv$cvm %>%  min
model = glmnet(x = model.matrix(~-1 + ., data = X),
               y = log(df_nona[['price']]), 
               alpha = 0, lambda = model_cv$lambda.min,
               standardize = TRUE,
               intercept = TRUE)
model$beta


# adaptive lasso 
lambda = exp(seq(-10, 10, length= 300))
lasso_model_cv = cv.glmnet(x = model.matrix(~-1 + ., data = X),
                     y = log(df_nona[['price']]), 
                     alpha = 1, lambda = lambda, 
                     standardize = TRUE,
                     intercept = TRUE,
                     penalty.factor = 1/abs(model$beta))
plot(lasso_model_cv)
lasso_model_cv$lambda.min
lasso_model_cv$cvm %>%  min
lasso_model = glmnet(x = model.matrix(~-1 + ., data = X),
               y = log(df_nona[['price']]), 
               alpha = 1, lambda = lasso_model_cv$lambda.min,
               standardize = TRUE,
               intercept = TRUE,
               penalty.factor = 1/abs(model$beta))
lasso_model$beta

# check the the two model on test data
df_test = readr::read_csv("test_with_feats.csv")
df_test = df_test |> 
  distinct() |>
  dplyr::select(colnames(df_nona))

df_test$finnhubIndustry = ifelse(is.na(df_test$finnhubIndustry), 'other', df_test$finnhubIndustry)
df_test = df_test %>%  na.omit

pred_no_imp = predict(
  lasso_model, 
  model.matrix(~ -1 + ., df_test |> dplyr::select(X %>%  colnames))
)

df_test$finnhubIndustry <- factor(df_test$finnhubIndustry, levels = unique(X_imp$finnhubIndustry))
pred_imp = predict(
  lasso_model_imputed, 
  model.matrix(~ -1 + ., df_test |> dplyr::select(X_imp %>%  colnames))
)

# calculate mse and mae
mse_imp = mean((log(df_test$price) - pred_imp)**2)
mse_no_imp = mean((log(df_test$price) - pred_no_imp)**2)

mae_imp = mean(abs(log(df_test$price) - pred_imp))
mae_no_imp = mean(abs(log(df_test$price) - pred_no_imp))

# mae on original scale
mse_imp_orig = mean(abs(df_test$price - exp(pred_imp)))
mse_no_imp_orig = mean(abs(df_test$price - exp(pred_no_imp)))

write.csv(df_no_na, "/Users/lucariotto/Documents/Personal/Gestione denaro/Company analysis/exploration/data/train_non_na.csv", row.names = FALSE)
