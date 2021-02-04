library(dplyr) ; library(lubridate) ; library(doParallel)

fls <- paste("./data/",seq(1,11845,1),".csv",sep="")
dt <- seq(as.Date("1950-01-01"), as.Date("2100-12-30"),1)

ff <- function(f) {
	d <- read.csv(f)
	colnames(d) <- c("rain", "snow", "swe", "runoff", "ros")
	d$dt <- dt
	d1 <- data.frame(d %>% filter(year(dt) > 1980) %>% filter(year(dt) <= 2010))
	d2 <- data.frame(d %>% filter(year(dt) > 2070) %>% filter(year(dt) <= 2100))
	d <- rbind(d1,d2)
	#d <- data.frame(d %>% filter(month(dt) %in% c(10,11,12,1,2,3)))
	d <- data.frame(d %>% group_by(year(dt), month(dt)) %>% summarise(rain = sum(rain), snow = sum(snow), swe = mean(swe), runoff = sum(runoff), ros = sum(ros)))
	# Summer
	d2 <- data.frame(d %>% filter(month.dt. %in% c(4:9)))
	d2 <- data.frame(d2 %>% group_by(year.dt.) %>% summarise(rain = sum(rain), snow = sum(snow), swe = mean(swe), runoff = sum(runoff), ros = sum(ros)))
	d2 <- d2[,2:6]
	d2$id <- c(seq(1981,2010,1),seq(2071,2100,1))
	# Winter
	d1 <- data.frame(d %>% filter(month.dt. %in% c(10,11,12,1,2,3)))
	d1 <- d1[-c(1:3,178:183,358:360), 3:7]
	d1$id <- rep(seq(1,length(d1$ros)/6,1),each=6)
	d1 <- data.frame(d1 %>% group_by(id) %>% summarise(rain = sum(rain), snow = sum(snow), swe = mean(swe), runoff = sum(runoff), ros = sum(ros)))
	d1$id <- c(seq(1982,2010,1),seq(2072,2100,1))
	d <- rbind(d2,d1) # 1:60 is summer, 61:118 is winter
	return(d)
}

cl <- makeCluster(30)
registerDoParallel(cl) 

resls <- foreach(f = fls,.packages=c('lubridate','dplyr'), .export = c('dt')) %dopar% {ff(f)}
stopCluster(cl)

saveRDS(resls, "rls.rds", compress=F)
q()