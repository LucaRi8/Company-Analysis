library(tidyverse)
library(boot)
df_nona = readr::read_csv("train_non_na.csv")


# use original data and drop NA rows
lambda = exp(seq(-10,5, length=100))
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

# bootstrap estimate for confidence intervals
lambda = exp(seq(-10, 10, length= 300))
N_df = nrow(df_nona)
boot_model = list()
pb <- txtProgressBar(min = 0, max = 500, style = 3)
for(i in 1:500)
{
  boot_df_nona = df_nona[sample(1:N_df, size=N_df, replace = T), ]
  boot_df_nona$finnhubIndustry = factor(boot_df_nona$finnhubIndustry, levels = unique(df_nona$finnhubIndustry))
  
  X = boot_df_nona |> 
    dplyr::select(-c(ticker, date_q, price))
  
  lasso_model_cv = cv.glmnet(x = model.matrix(~-1 + ., data = X),
                             y = log(boot_df_nona[['price']]), 
                             alpha = 1, lambda = lambda, 
                             standardize = TRUE,
                             intercept = TRUE,
                             penalty.factor = 1/abs(model$beta))

  lasso_model = glmnet(x = model.matrix(~-1 + ., data = X),
                       y = log(boot_df_nona[['price']]), 
                       alpha = 1, lambda = lasso_model_cv$lambda.min,
                       standardize = TRUE,
                       intercept = TRUE,
                       penalty.factor = 1/abs(model$beta))
  boot_model[[i]] = lasso_model
  setTxtProgressBar(pb, i)
}

# pred int on testset
df_test = readr::read_csv("test_with_feats.csv")
df_test = df_test |> 
  distinct() |>
  dplyr::select(colnames(df_nona))

df_test$finnhubIndustry = ifelse(is.na(df_test$finnhubIndustry), 'other', df_test$finnhubIndustry)
df_test = df_test %>%  na.omit
df_test$finnhubIndustry = factor(df_test$finnhubIndustry, levels = unique(df_nona$finnhubIndustry))
X_test = df_test |> dplyr::select(-c(ticker, date_q, price))
X_test = model.matrix(~., X_test)
pred_matrix = do.call(cbind, lapply(boot_model, function(m) exp(predict(m, X_test))))

CI_test = apply(pred_matrix, 1, function(x) quantile(x, c(0.025, 0.975))) %>%  t
test_perf = df_test |> dplyr::select(ticker, date_q, price)
test_perf[, c('pred', 'clow', 'cup')] = cbind(exp(predict(lasso_model, X_test)), CI_test)

test_perf = test_perf |>
  left_join(
    test_perf |> 
      dplyr::select(ticker, date_q, price) |>
      mutate(date_q = as.Date(date_q)  %m+% months(3)) |>
      rename(c('price_tm1' = price)),
    join_by(ticker, date_q)
  )

test_perf = 
  test_perf |>
  mutate(is_not_between = !between(price_tm1, clow, cup))

test_perf$is_not_between %>%  sum(na.rm=T)

test_perf[which(test_perf$is_not_between), ]
