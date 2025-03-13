library(tidyverse)
library(ggplot2)
library(ggpubr)

# load the data
df = readr::read_csv("target_with_feats.csv")
df
dim(df)
summary(df)

# high number of null value, check if missing are random respect to price feature
summary(lm(price ~ is.na(ebitPerShare), data = df))
summary(lm(price ~ is.na(fcfPerShare), data = df))
summary(lm(price ~ is.na(salesPerShare), data = df))
summary(lm(price ~ is.na(ev_sector_ratio), data = df))

# companies with lower price have more probability to have missing data

na_tick = df |>
  group_by(ticker) |>
  summarise(avg_na = mean(is.na(ebitPerShare))) |>
  arrange(desc(avg_na)) |>
  filter(avg_na == 1) |> 
  select(ticker)

summary(df[df$ticker %in% na_tick[['ticker']], ])
# lots of companies have null in most of the features


# find the features most correlated with the response variable
cor_feats = cor(df |> select(- c('date_q', 'ticker', 'finnhubIndustry')) |> drop_na())
cor_feats_name = names(sort(cor_feats['price', ], decreasing = TRUE)[1:6])

cor_long <- as.data.frame(as.table(cor_feats[cor_feats_name, cor_feats_name])) %>%
  rename(x = Var1, y = Var2, correlation = Freq)

ggplot(cor_long, aes(x = x, y = y, fill = correlation)) +
  geom_tile() +
  scale_fill_viridis_c() +  
  theme_minimal() +        
  labs(title = "Heatmap of most most corr feats",
       x = "",
       y = "",
       fill = "correlaion") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))  

cor_feats[cor_feats_name, cor_feats_name]

# delete recors with negative price
df = df |> filter(price > 0)

# scatter with the most correlaed feats with price

p1 = df |>
  filter(ebitPerShare > 0 & ebitPerShare < 30) |>
  ggplot(aes(x = ebitPerShare, y = price)) + 
  geom_point() + 
  geom_smooth()

p2 = df |>
  filter(eps > 0 & eps < 30) |>
  ggplot(aes(x = eps, y = price)) + 
  geom_point() + 
  geom_smooth()

p3 = df |>
  filter(fcfPerShare > 0 & fcfPerShare < 30) |>
  ggplot(aes(x = fcfPerShare, y = price)) + 
  geom_point() + 
  geom_smooth()

p4 = df |>
  filter(ev_sector_ratio > 0 & ev_sector_ratio < 30) |>
  ggplot(aes(x = log(salesPerShare), y = log(price))) + 
  geom_point() + 
  geom_smooth()

p5 = df |>
  filter(ev_sector_ratio > 0) |>
  ggplot(aes(x = log(ev_sector_ratio), y = log(price))) + 
  geom_point() + 
  geom_smooth()

ggarrange(p1, p2, p3, p4, p5, ncol = 3, nrow = 2)










