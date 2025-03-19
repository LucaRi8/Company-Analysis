library(tidyverse)
library(ggplot2)
library(ggpubr)
library(MASS)
library(e1071)

# load the data
df = readr::read_csv("target_with_feats.csv")
df = df |> distinct()
df
dim(df)
summary(df)

# high number of null value, check if missing are random respect to price feature
summary(lm(price ~ is.na(ebitPerShare), data = df))
summary(lm(price ~ is.na(fcfPerShare), data = df))
summary(lm(price ~ is.na(salesPerShare), data = df))
summary(lm(price ~ is.na(ev_sector_ratio), data = df))

# companies with lower price have more probability to have missing data

na_tick_ebit = df |>
  group_by(ticker) |>
  summarise(avg_na = mean(is.na(ebitPerShare))) |>
  arrange(desc(avg_na)) |>
  filter(avg_na == 1) |> 
  dplyr::select(ticker)

summary(df[df$ticker %in% na_tick_ebit[['ticker']], ])
# lots of companies have null in most of the features

features_name = df %>%  names %>% setdiff(c('date_q', 'ticker', 'price'))
df = df |> filter((!if_all(features_name, is.na)) & !(ticker %in% na_tick_ebit[['ticker']]))

# find the features most correlated with the response variable
cor_feats = cor(df |> dplyr::select(- c('date_q', 'ticker', 'finnhubIndustry')) |> drop_na())
cor_feats_name = names(sort(cor_feats['price', ], decreasing = TRUE)[1:6])

cor_long <- as.data.frame(as.table(cor_feats[cor_feats_name, cor_feats_name])) %>%
  rename(x = Var1, y = Var2, correlation = Freq)

ggplot(cor_long, aes(x = x, y = y, fill = correlation)) +
  geom_tile() +
  scale_fill_viridis_c() +  
  theme_minimal() +        
  labs(title = "Heatmap of most corr feats",
       x = "",
       y = "",
       fill = "correlaion") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))  

cor_feats[cor_feats_name, cor_feats_name]


# scatter with the most correlaed feats with price
# there are some outlier in both covariate and target variable, 
# so some filter are necessary
df = df |> filter(price > 0 & price < 10000)
p1 = df |>
  filter(ebitPerShare > 0 & ebitPerShare < 30) |>
  ggplot(aes(x = ebitPerShare, y = log(price))) + 
  geom_point() + 
  geom_smooth()

p2 = df |>
  filter(eps > 0 & eps < 30) |>
  ggplot(aes(x = eps, y = log(price))) + 
  geom_point() + 
  geom_smooth()

p3 = df |>
  filter(fcfPerShare > 0 & fcfPerShare < 30) |>
  ggplot(aes(x = fcfPerShare, y = log(price))) + 
  geom_point() + 
  geom_smooth()

p4 = df |>
  filter(salesPerShare > 0 & salesPerShare < 30) |>
  ggplot(aes(x = log(salesPerShare), y = log(price))) + 
  geom_point() + 
  geom_smooth()

p5 = df |>
  filter(ev_sector_ratio > 0) |>
  ggplot(aes(x = log(ev_sector_ratio), y = log(price))) + 
  geom_point() + 
  geom_smooth()


ggarrange(p1, p2, p3, p4, p5, ncol = 3, nrow = 2)


# compute a simple linear model 
df_filtered = df |> 
  filter(
    ev_sector_ratio > 0 & 
    salesPerShare > 0 & 
    ebitPerShare > 0 & ebitPerShare < 30 & 
    eps > 0 & eps < 30 & 
    fcfPerShare > 0 & fcfPerShare < 30
)
model = lm(
  log(price) ~ ebitPerShare + eps + log(salesPerShare) +
  fcfPerShare + log(ev_sector_ratio) + finnhubIndustry, 
  data = df_filtered
)
summary(model)


# boxplot of some features
quartz()
par(mfrow = c(4, 5))
numeric_feats = names(df[, sapply(df, class) == "numeric"][1:20])
for(feat in numeric_feats)
  boxplot(df[,feat], xlab = as.character(feat))

# density of price
df |>
  ggplot(aes(x = price)) +
  geom_density()

df |>
  mutate(norm_price = rnorm(nrow(df), mean = mean(log(df$price)), sd = sd(log(df$price)))) |>
  ggplot(aes(x = log(price))) +
  geom_density() + 
  geom_density(aes(x = norm_price, color='red'))

ks.test(log(df$price), 'pnorm')
kurtosis(log(df$price))
skewness(log(df$price))

# correlation between covariates
cor_feats = cor(df |> dplyr::select(- c('date_q', 'ticker', 'finnhubIndustry')) |> drop_na())
cor_feats_name = names(sort(cor_feats['price', ], decreasing = TRUE))

cor_long <- as.data.frame(as.table(cor_feats[cor_feats_name, cor_feats_name])) %>%
  rename(x = Var1, y = Var2, correlation = Freq)

ggplot(cor_long, aes(x = x, y = y, fill = correlation)) +
  geom_tile() +
  scale_fill_viridis_c() +  
  theme_minimal() +        
  labs(title = "Heatmap of feats corr",
       x = "",
       y = "",
       fill = "correlaion") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))  

# acf of price for some companies
tk = df |> group_by(ticker) |> summarise(count=n()) |> arrange(desc(count)) 
tk = tk[["ticker"]][1:9]
df |>
  filter(ticker %in% tk) |>
  ggplot(aes(x = date_q, y = log(price), group = ticker, color = ticker)) +
  geom_line()

par(mfrow = c(3,3))
for(i in tk)
  df |> filter(ticker == i) |> dplyr::select(price) |> mutate(price=log(price)) %>%  acf

par(mfrow = c(1,1))

# use t-1 price to control for autocorreelated data
df = df |>
  left_join(
    df |> 
      dplyr::select(ticker, date_q, price) |>
      mutate(date_q = as.Date(date_q)  %m+% months(3)) |>
      rename(c('price_tm1' = price)),
    join_by(ticker, date_q)
  )


model = lm(log(price) ~ offset(1*log(price_tm1)) + 
             #log(price_tm1) + 
             ebitPerShare + eps + cashRatio + fcfPerShare +
             `ebitPerShare__agg_linear_trend__attr_"slope"__chunk_len_1__f_agg_"mean"` +
             roa + bookValue, data = df)
summary(model)
plot(model)
