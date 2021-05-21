library(dplyr) ; library(lubridate) ; library(doParallel)

fls <- paste("./data/",seq(1,11845,1),".csv",sep="")
dt <- seq(as.Date("1950-01-01"), as.Date("2100-12-30"),1)
dir <- sub(".*/", "", getwd())
yrs <- vector(mode="list", length=14)
names(yrs) <- c("flux_mbcn_ACCESS1-3_rcp45", "flux_mbcn_ACCESS1-3_rcp85",
		"flux_mbcn_canESM2_rcp45", "flux_mbcn_canESM2_rcp85",
		"flux_mbcn_CNRM_rcp45", "flux_mbcn_CNRM_rcp85",
		"flux_mbcn_GFDL_rcp45", "flux_mbcn_GFDL_rcp85",
		"flux_mbcn_HadGEM_rcp45","flux_mbcn_HadGEM_rcp85",
		"flux_mbcn_MPI-ESM-LR_rcp45","flux_mbcn_MPI-ESM-LR_rcp85",
		"flux_mbcn_MPI-ESM-MR_rcp45","flux_mbcn_MPI-ESM-MR_rcp85")

yrs[[1]] <- c(2007,2025,2040,2064)
yrs[[2]] <- c(2005,2017,2025,2033)
yrs[[3]] <- c(2008,2023,2044,NA)
yrs[[4]] <- c(2003,2016,2028,2038)
yrs[[5]] <- c(2023,2045,NA,NA)
yrs[[6]] <- c(2016,2030,2042,2053)
yrs[[7]] <- c(2035,NA,NA,NA)
yrs[[8]] <- c(2023,2038,2053,2068)
yrs[[9]] <- c(2007,2024,2039,2055)
yrs[[10]] <- c(2004,2016,2027,2036)
yrs[[11]] <- c(2019,2049,NA,NA)
yrs[[12]] <- c(2012,2029,2040,2052)
yrs[[13]] <- c(2021,2045,NA,NA)
yrs[[14]] <- c(2015,2030,2040,2051)

index_1 <- function(d,y) {
	i1 <- c()
	for (yr in y:(y+29)) {
		dsub <- data.frame(d %>% filter(year(dt) == yr) %>% filter(rain >= 1))
		i1 <- c(i1, nrow(dsub))
	}
	return(i1)
}

index_2 <- function(d,y) {
	i2 <- c()
	for (yr in y:(y+29)) {
		dsub <- data.frame(d %>% filter(year(dt) == yr) %>% filter(rain >= 1))
		denom <- nrow(dsub)
		dsub <- data.frame(d %>% filter(year(dt) == yr) %>% filter(rain >= 3) %>% filter(swe >= 5))
		nomin <- nrow(dsub)
		i2 <- c(i2, nomin/denom)
	}
	return(round(i2,2))
}

index_3 <- function(d,y) {
	i3 <- c()
	for (yr in y:(y+29)) {
		dsub <- data.frame(d %>% filter(year(dt) == yr) %>% filter(rain >= 1))
		denom <- nrow(dsub)
		dsub <- data.frame(d %>% filter(year(dt) == yr) %>% filter(rain >= 10) %>% filter(swe >= 5))
		nomin <- nrow(dsub)
		i3 <- c(i3, nomin/denom)
	}
	return(round(i3,2))
}

index_4 <- function(d,y) {
	i4 <- c()
	for (yr in y:(y+29)) {
		dsub <- data.frame(d %>% filter(year(dt) == yr) %>% filter(rain >= 3) %>% filter(swe >= 5))
		i4 <- c(i4, sum(dsub$rain)/nrow(dsub))
	}
	return(round(i4,2))
}

index_5 <- function(d,y) {
	i5 <- c()
	for (yr in y:(y+29)) {
		dsub <- data.frame(d %>% filter(year(dt) == yr) %>% filter(rain >= 10) %>% filter(swe >= 5))
		i5 <- c(i5, sum(dsub$rain)/nrow(dsub))
	}
	return(round(i5,2))
}

index_6 <- function(d,y) {
	i6 <- c()
	for (yr in y:(y+29)) {
		dsub <- data.frame(d %>% filter(year(dt) == yr))
		days <- 1:nrow(dsub)
		ros_days <- which(dsub$rain >= 3 & dsub$swe >= 5)
		no_ros_days <- setdiff(days, ros_days)
		runoff_inc <- (mean(dsub$runoff[ros_days]) - mean(dsub$runoff[no_ros_days]))/mean(dsub$runoff[no_ros_days])
		i6 <- c(i6, runoff_inc)
	}
	return(round(i6,2))
}

index_7 <- function(d,y) {
	i7 <- c()
	for (yr in y:(y+29)) {
		dsub <- data.frame(d %>% filter(year(dt) == yr))
		days <- 1:nrow(dsub)
		ros_days <- which(dsub$rain >= 10 & dsub$swe >= 5)
		no_ros_days <- setdiff(days, ros_days)
		runoff_inc <- (mean(dsub$runoff[ros_days]) - mean(dsub$runoff[no_ros_days]))/mean(dsub$runoff[no_ros_days])
		i7 <- c(i7, runoff_inc)
	}
	return(round(i7,2))
}

index_8 <- function(d,y) {
	i8 <- c()
	for (yr in y:(y+29)) {
		dsub <- data.frame(d %>% filter(year(dt) == yr))
		days <- 1:nrow(dsub)
		swe_change <- c(0)
		for (i in 1:(length(dsub$swe)-1)) {
			swe_change <- c(swe_change, dsub$swe[i+1] - dsub$swe[i])
		}
		ros_days <- which(dsub$rain >= 3 & dsub$swe >= 5)
		no_ros_days <- setdiff(days, ros_days)
		swe_melt_ros_days <- mean(dsub$swe[ros_days])
		swe_melt_no_ros_days <- mean(dsub$swe[no_ros_days])
		i8 <- c(i8, swe_melt_ros_days/swe_melt_no_ros_days)
	}
	return(round(i8,2))
}

index_9 <- function(d,y) {
	i9 <- c()
	for (yr in y:(y+29)) {
		dsub <- data.frame(d %>% filter(year(dt) == yr))
		days <- 1:nrow(dsub)
		swe_change <- c(0)
		for (i in 1:(length(dsub$swe)-1)) {
			swe_change <- c(swe_change, dsub$swe[i+1] - dsub$swe[i])
		}
		ros_days <- which(dsub$rain >= 10 & dsub$swe >= 5)
		no_ros_days <- setdiff(days, ros_days)
		swe_melt_ros_days <- mean(dsub$swe[ros_days])
		swe_melt_no_ros_days <- mean(dsub$swe[no_ros_days])
		i9 <- c(i9, swe_melt_ros_days/swe_melt_no_ros_days)
	}
	return(round(i9,2))
}


ff <- function(f) {
	d <- read.csv(f)
	colnames(d) <- c("rain", "snow", "swe", "runoff", "ros")
	d$dt <- dt
	

	func_warm_prd <- function(y, mons) {
		if (is.na(y)) {
			return(NA)
		}
		df <- data.frame(d %>% filter(year(dt) >= y) %>% filter(year(dt) < y+30))
		df <- data.frame(df %>% filter(month(dt) %in% mons))
		i1 <- index_1(df,y)
		i2 <- index_2(df,y)
		i3 <- index_3(df,y)
		i4 <- index_4(df,y)
		i5 <- index_5(df,y)
		i6 <- index_6(df,y) # Runoff generated on ROS (3mm) days is X times more than non-ros days
		i7 <- index_7(df,y) # Runoff generated on ROS (10mm) days is X times more than non-ros days
		i8 <- index_8(df,y) # Snowmelt on ROS days (3mm) divided by snowmelt on non-ros days
		i9 <- index_9(df,y) # Snowmelt on ROS days (10mm) divided by snowmelt on non-ros days
		
		df <- data.frame(cbind(i1,i2,i3,i4,i5,i6,i7,i8,i9))
		return(as.numeric(colMeans(df,na.rm=T)))
	}

	dls <- list()
	for (mons in list(c(1,2,3), c(4,5,6), c(7,8,9), c(10,11,12))) {
		base <- func_warm_prd(1976, mons)
		w1 <- func_warm_prd(yrs[[dir]][1], mons)
		w2 <- func_warm_prd(yrs[[dir]][2], mons)
		w3 <- func_warm_prd(yrs[[dir]][3], mons)
		w4 <- func_warm_prd(yrs[[dir]][4], mons)
		dls[[length(dls)+1]] <- rbind(base,w1,w2,w3,w4)
	}

	return(dls)
}

cl <- makeCluster(30)
registerDoParallel(cl) 
resls <- foreach(f = fls,.packages=c('lubridate','dplyr'), .export = c('dt','yrs','dir')) %dopar% {ff(f)}
stopCluster(cl)

saveRDS(resls, paste("../resdir/",dir,".rds",sep=""), compress=F)
q()