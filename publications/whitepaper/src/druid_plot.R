library(ggplot2)
library(stringr)
library(plyr)

x <-
"Dimension	Cardinality	Concise compressed size (bytes)
Has_mention	2	586,400
Has_links	2	580,872
Has_geo	2	144,004
Is_retweet	2	584,592
Is_viral	2	358,380
User_lang	21	1,414,000
User_time_zone	142	3,876,244
URL_domain	31,165	1,562,428
First_hashtag	100,728	1,837,144
Rt_name	182,704	2,235,288
Reply_to_name	620,421	5,673,504
User_location	637,774	9,511,844
User_mention_name	923,842	9,086,416
User_name	1,784,369	16,000,028"

foo <- function(x){
  m <- matrix(unlist(str_split(x, "\t|\n")), ncol = 3, byrow = TRUE)
  df <- data.frame(m[-1, ], stringsAsFactors = FALSE)
  names(df) <- m[1, ]
  df[, 2] <- as.numeric(str_replace_all(df[, 2], ",", ""))
  df[, 3] <- as.numeric(str_replace_all(df[, 3], ",", ""))
  df <- transform(df, ytext = `Concise\ compressed\ size\ (bytes)`)
  names(df) <- c(m[1, ], "ytext")
  df
}
df <- foo(x)
## df$ytext[12] <- 1.05 * df$ytext[12]
## df$ytext[13] <- .93 * df$ytext[13]
## df$ytext[1] <- 1.13 * df$ytext[1]
## df$ytext[4] <- .87 * df$ytext[4]

## qplot(x = Cardinality, y = `Concise\ compressed\ size\ (bytes)`, data = df, geom = "point") +
##   geom_text(aes(x = Cardinality * 1.2, y = ytext, label = Dimension), hjust = 0, size = 4) +
##   scale_x_log10(limits = c(1, 10^8.5)) + 
##   scale_y_log10() +
##   geom_hline(aes(yintercept = 9089180)) +
##   geom_text(aes(x = 1e2, y = 9089180 * 1.1, label = "Integer array size (bytes)"), hjust = 0, size = 4) +
##   ggtitle("The Relationship of Compressed Size to Cardinality")

  
y <- 
"Dimension	Cardinality	Concise compressed size (bytes)
Has_mention	2	744
Has_links	2	1,504
Has_geo	2	2,840
Is_retweet	2	1,616
Is_viral	2	1,488
User_lang	21	38,416
User_time_zone	142	319,644
URL_domain	31,165	700,752
First_hashtag	100,728	1,505,292
Rt_name	182,704	1,874,180
Reply_to_name	620,421	5,404,108
User_location	637,774	9,091,016
User_mention_name	923,842	8,686,384
User_name	1,784,369	16,204,900"
df2 <- foo(y)

df$sorted <- "unsorted"
df2$sorted <- "sorted"
dat <- rbind(df, df2)


ggplot(data = dat, aes(x = Cardinality, y = `Concise\ compressed\ size\ (bytes)`)) +
  geom_point(aes(color = sorted, shape = sorted), alpha = .9, size = 4) + 
  scale_x_log10(limits = c(1, 10^8.5)) + 
  scale_y_log10() +
  geom_hline(aes(yintercept = 9089180)) +
  geom_text(aes(x = 1e1, y = 9089180 * 1.4, label = "Integer array size (bytes)"), hjust = 0, size = 5)
#ggsave("concise_plot.png", width = 10, height = 8)
ggsave("../figures/concise_plot.pdf", width = 6, height = 4.5)
