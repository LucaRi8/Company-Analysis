library(tidyverse)
library(ggplot2)
library(ggpubr)

# load the data
df = readr::read_csv("target_with_feats.csv")
df

# find the features most correlated with the response variable
cor_feats = cor(df |> select(- c('date_q', 'ticker', 'finnhubIndustry')))['price', ]
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

# scatter with the most correlaed feats with price
p1 = df |>
  ggplot(aes(x = ebitPerShare, y = price)) + 
  geom_point() + 
  geom_smooth()

p2 = df |>
  ggplot(aes(x = eps, y = price)) + 
  geom_point() + 
  geom_smooth()

p3 = df |>
  ggplot(aes(x = fcfPerShare, y = price)) + 
  geom_point() + 
  geom_smooth()

p4 = df |>
  ggplot(aes(x = salesPerShare, y = price)) + 
  geom_point() + 
  geom_smooth()

p5 = df |>
  ggplot(aes(x = ev_sector_ratio, y = price)) + 
  geom_point() + 
  geom_smooth()

ggarrange(p1, p2, p3, p4, p5, ncol = 3, nrow = 2)

p1 = df |>
  ggplot(aes(x = ebitPerShare, y = log(price))) + 
  geom_point() + 
  geom_smooth()

p2 = df |>
  ggplot(aes(x = eps, y = log(price))) + 
  geom_point() + 
  geom_smooth()

p3 = df |>
  ggplot(aes(x = fcfPerShare, y = log(price))) + 
  geom_point() + 
  geom_smooth()

p4 = df |>
  ggplot(aes(x = log(salesPerShare), y = log(price))) + 
  geom_point() + 
  geom_smooth()

p5 = df |>
  ggplot(aes(x = log(ev_sector_ratio), y = log(price))) + 
  geom_point() + 
  geom_smooth()

ggarrange(p1, p2, p3, p4, p5, ncol = 3, nrow = 2)

# simple linear model
model = lm(log(price) ~ log(ev_sector_ratio) + log(salesPerShare) + 
             fcfPerShare + ebitPerShare + eps, df)
summary(model)
