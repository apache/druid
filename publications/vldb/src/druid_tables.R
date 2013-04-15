library(stringr)
library(xtable)
library(plyr)
library(ggplot2)

stringToDF <- function(x, ncol){
  m <- matrix(unlist(str_split(x, "\t|\n")), ncol = ncol, byrow = TRUE)
  df <- data.frame(m[-1, ], stringsAsFactors = FALSE)
  names(df) <- m[1, ]
  df
}

##print(xtable(stringToDF(x, 3)), include.rownames = FALSE)

stringToDF2 <- function(x, query){
  m <- matrix(unlist(str_split(x, "\t|\n")), ncol = 3, byrow = TRUE)
  df <- data.frame(m[-1, ], stringsAsFactors = FALSE)
  names(df) <- m[1, ]
  df[, 2] <- as.numeric(str_replace_all(df[, 2], ",", ""))
  df[, 3] <- as.numeric(str_replace_all(df[, 3], ",", ""))
  df2 <- ldply(1:nrow(df), function(i) gsub("^\\s+|\\s+$", "", str_split(df[i, 1], ",")[[1]]))
  names(df2) <- c("cores", "nodes", "configuration")
  df2$query <- query
  cbind(df, df2)
}

x2 <- "Cluster
Cluster scan rate (rows/sec)
Core scan rate
15-core, 100 nodes, in-memory 
26,610,386,635
17,740,258
15-core, 75 nodes, mmap
25,224,873,928
22,422,110
15-core, 50 nodes, mmap
20,387,152,160
27,182,870
15-core, 25 nodes, mmap
11,910,388,894
31,761,037
4-core, 131 nodes, in-memory
10,008,730,163
19,100,630
4-core, 131 nodes, mmap
10,129,695,120
19,331,479
4-core, 50 nodes, mmap
6,626,570,688
33,132,853"


x3 <- "Cluster
Cluster scan rate (rows/sec)
Core scan rate
15-core, 100 nodes, in-memory 
16,223,081,703 
10,815,388
15-core, 75 nodes, mmap
9,860,968,285
8,765,305
15-core, 50 nodes, mmap
8,093,611,909
10,791,483
15-core, 25 nodes, mmap
4,126,502,352
11,004,006
4-core, 131 nodes, in-memory
5,755,274,389
10,983,348
4-core, 131 nodes, mmap
5,032,185,657
9,603,408
4-core, 50 nodes, mmap
1,720,238,609
8,601,193"

x4 <- "Cluster
Cluster scan rate (rows/sec)
Core scan rate
15-core, 100 nodes, in-memory 
7,591,604,822
5,061,070
15-core, 75 nodes, mmap
4,319,179,995
3,839,271
15-core, 50 nodes, mmap
3,406,554,102
4,542,072
15-core, 25 nodes, mmap
1,826,451,888
4,870,538
4-core, 131 nodes, in-memory
1,936,648,601
3,695,894
4-core, 131 nodes, mmap
2,210,367,152
4,218,258
4-core, 50 nodes, mmap
1,002,291,562
5,011,458"

x5 <- "Cluster
Cluster scan rate (rows/sec)
Core scan rate
15-core, 100 nodes, in-memory 
10,241,183,745
6,827,456
15-core, 75 nodes, mmap
4,891,097,559
4,347,642
15-core, 50 nodes, mmap
3,616,707,511
4,822,277
15-core, 25 nodes, mmap
1,665,053,263
4,440,142
4-core, 131 nodes, in-memory
4,388,159,569
8,374,350
4-core, 131 nodes, mmap
2,444,344,232
4,664,779
4-core, 50 nodes, mmap
1,215,737,558
6,078,688"

x6 <- "Cluster
Cluster scan rate (rows/sec)
Core scan rate
15-core, 100 nodes, in-memory 
7,309,984,688
4,873,323
15-core, 75 nodes, mmap
3,333,628,777
2,963,226
15-core, 50 nodes, mmap
2,555,300,237
3,407,067
15-core, 25 nodes, mmap
1,384,674,717
3,692,466
4-core, 131 nodes, in-memory
3,237,907,984
6,179,214
4-core, 131 nodes, mmap
1,740,481,380
3,321,529
4-core, 50 nodes, mmap
863,170,420
4,315,852"

x7 <- "Cluster
Cluster scan rate (rows/sec)
Core scan rate
15-core, 100 nodes, in-memory 
4,064,424,274                         
2,709,616
15-core,  75 nodes, mmap
2,014,067,386
1,790,282
15-core,  50 nodes, mmap
1,499,452,617
1,999,270
15-core,  25 nodes, mmap
810,143,518
2,160,383
4-core, 131 nodes, in-memory
1,670,214,695
3,187,433
4-core, 131 nodes, mmap
1,116,635,690
2,130,984
4-core, 50 nodes, mmap
531,389,163
2,656,946"

dat <- ldply(2:7, function(i){
  df <- eval(parse(text = paste("x", i, sep = "")))
  stringToDF2(df, paste("Query", i - 1, sep = " "))
})

ggplot(data = dat, aes(
         x = as.numeric(str_extract(nodes, "[0-9]+")),
         y = `Cluster scan rate (rows/sec)` / 1e9,
         shape = configuration,
         color = query,
         size = factor(cores, levels = c("4-core", "15-core"))
         )) +
  scale_size_manual(values = c(3, 6), guide = guide_legend(title = "Cores Per Node")) +
  scale_color_discrete(guide = guide_legend(title = "Query")) +
  scale_shape_discrete(guide = guide_legend(title = "Configuration")) + 
  geom_point() +
  scale_y_log10(breaks = 2^(-1:5)) +
  facet_grid(configuration~.) + 
  xlab("Number of Nodes") +
  ylab("Cluster Scan Rate (billion rows/sec.)")

dat2 <- subset(dat, cores == "15-core" & configuration == "mmap")
dat2$x <- as.numeric(str_extract(dat2$nodes, "[0-9]+"))
baseRate <- list()
d_ply(subset(dat2, x == 25), .(query), function(df) baseRate[df$query] <<- df$`Cluster scan rate (rows/sec)`)
dat2 <- ddply(dat2, .(query, x), function(df){
  df$projected <- df$x / 25 * unlist(baseRate[df$query])
  df
})

ggplot(data = dat2, aes(
         x = as.numeric(str_extract(nodes, "[0-9]+")),
         y = `Cluster scan rate (rows/sec)` / 1e9,
         color = query,
         shape = query
         )) +
#    scale_y_log10(breaks = 2^(-1:5)) +
#  scale_color_discrete(guide = guide_legend(title = "Query")) +
  geom_point(size = 3) +
#  geom_path(aes(y = projected)) +
  geom_line(aes(y = projected / 1e9)) + 
#  scale_y_log10() +
  xlab("Number of Nodes") +
  ylab("Cluster Scan Rate (billion rows/sec.)")
ggsave("../figures/cluster_scan_rate.pdf", width = 4, height = 3)



## ggplot(data = dat, aes(
##          x = as.numeric(str_extract(nodes, "[0-9]+")),
##          y = `Core scan rate` / 1e6,
##          shape = configuration,
##          color = query,
##          size = factor(cores, levels = c("4-core", "15-core"))
##          )) +
##   scale_size_manual(values = c(3, 6), guide = guide_legend(title = "Cores Per Node")) +
##   scale_color_discrete(guide = guide_legend(title = "Query")) +
##   scale_shape_discrete(guide = guide_legend(title = "Configuration")) + 
##   geom_point() +
##   scale_y_log10(breaks = 2^(-1:5)) +
##   xlab("Number of Nodes") +
##   ylab("Core Scan Rate (million rows/sec.)")

ggplot(data = dat2, aes(
         x = as.numeric(str_extract(nodes, "[0-9]+")),
         y = `Core scan rate` / 1e6,
         color = query,
         shape = query
         )) +

  geom_point(size = 3) +
  xlab("Number of Nodes") +
  ylab("Core Scan Rate (million rows/sec.)")
ggsave("../figures/core_scan_rate.pdf", width = 4, height = 3)


