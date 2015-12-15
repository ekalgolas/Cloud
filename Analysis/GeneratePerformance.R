# Read tables
data.gfs = read.table("performance_gfs.csv", sep = ",");
data.nfs = read.table("performance_nfs.csv", sep = ",");
data.ceph = read.table("performance_ceph_2replica.csv", sep = ",");
data.ceph.replica = read.table("performance_ceph.csv", sep = ",");

# Get valid and invalid data
data.gfs.valid = data.gfs[which(data.gfs$V5 == 'SUCCESS') , ]
data.gfs.invalid = data.gfs[which(data.gfs$V5 != "SUCCESS") , ]
data.nfs.valid = data.nfs[which(data.nfs$V5 == 'SUCCESS') , ]
data.nfs.invalid = data.nfs[which(data.nfs$V5 != "SUCCESS") , ]
data.ceph.valid = data.ceph[which(data.ceph$V5 == 'SUCCESS' | data.ceph$V5 == '') , ]
data.ceph.invalid = data.ceph[which(data.ceph$V5 != "SUCCESS" & data.ceph$V5 != '') , ]
data.ceph.replica.valid = data.ceph.replica[which(data.ceph.replica$V5 == 'SUCCESS' | data.ceph.replica$V5 == '') , ]

# Get data for each command
data.gfs.ls = data.gfs.valid[which(data.gfs.valid$V1 == 'LS') , ]
data.gfs.lsl = data.gfs.valid[which(data.gfs.valid$V1 == 'LSL') , ]
data.gfs.cd = data.gfs.valid[which(data.gfs.valid$V1 == 'CD') , ]
data.gfs.touch = data.gfs.valid[which(data.gfs.valid$V1 == 'TOUCH') , ]
data.gfs.mkdir = data.gfs.valid[which(data.gfs.valid$V1 == 'MKDIR') , ]
data.gfs.rmdir = data.gfs.valid[which(data.gfs.valid$V1 == 'RMDIR') , ]
data.gfs.rmdirf = data.gfs.valid[which(data.gfs.valid$V1 == 'RMDIRF') , ]

data.nfs.ls = data.nfs.valid[which(data.nfs.valid$V1 == 'LS') , ]
data.nfs.lsl = data.nfs.valid[which(data.nfs.valid$V1 == 'LSL') , ]
data.nfs.cd = data.nfs.valid[which(data.nfs.valid$V1 == 'CD') , ]
data.nfs.touch = data.nfs.valid[which(data.nfs.valid$V1 == 'TOUCH') , ]
data.nfs.mkdir = data.nfs.valid[which(data.nfs.valid$V1 == 'MKDIR') , ]
data.nfs.rmdir = data.nfs.valid[which(data.nfs.valid$V1 == 'RMDIR') , ]
data.nfs.rmdirf = data.nfs.valid[which(data.nfs.valid$V1 == 'RMDIRF') , ]

data.ceph.ls = data.ceph.valid[which(data.ceph.valid$V1 == 'LS') , ]
data.ceph.lsl = data.ceph.valid[which(data.ceph.valid$V1 == 'LSL') , ]
data.ceph.cd = data.ceph.valid[which(data.ceph.valid$V1 == 'CD') , ]
data.ceph.touch = data.ceph.valid[which(data.ceph.valid$V1 == 'TOUCH') , ]
data.ceph.mkdir = data.ceph.valid[which(data.ceph.valid$V1 == 'MKDIR') , ]
data.ceph.rmdir = data.ceph.valid[which(data.ceph.valid$V1 == 'RMDIR') , ]
data.ceph.rmdirf = data.ceph.valid[which(data.ceph.valid$V1 == 'RMDIRF') , ]

# Create boxplot for valid and invalid commands
jpeg("GFS - Valid vs. Invalid.jpg", width = 1080, height = 480)
par(mfrow = c(1, 2))
boxplot(data.gfs.valid$V4, main = "Valid Commands Performance for GFS", ylab = "Time in milliseconds")
boxplot(data.gfs.invalid$V4, main = "Invalid Commands Performance for GFS", ylab = "Time in milliseconds")
dev.off()

jpeg("NFS - Valid vs. Invalid.jpg", width = 1080, height = 480)
par(mfrow = c(1, 2))
boxplot(data.nfs.valid$V4, main = "Valid Commands Performance for NFS", ylab = "Time in milliseconds")
boxplot(data.nfs.invalid$V4, main = "Invalid Commands Performance for NFS", ylab = "Time in milliseconds")
dev.off()

jpeg("Ceph - Valid vs. Invalid.jpg", width = 1080, height = 480)
par(mfrow = c(1, 2))
boxplot(data.ceph.valid$V4, main = "Valid Commands Performance for Ceph", ylab = "Time in milliseconds")
boxplot(data.ceph.invalid$V4, main = "Invalid Commands Performance for Ceph", ylab = "Time in milliseconds")
dev.off()

# Create boxplots for each solution
jpeg("Overall performance.jpg", width = 1080, height = 480)
par(mfrow = c(1, 3))
boxplot(data.gfs.valid$V4, main = "GFS", ylab = "Time in milliseconds")
boxplot(data.nfs.valid$V4, main = "NFS", ylab = "Time in milliseconds")
boxplot(data.ceph.valid$V4, main = "Ceph", ylab = "Time in milliseconds")
dev.off()

# Create boxplots for command wise performance
jpeg("GFS - Command wise performance.jpg", width = 1080, height = 480)
par(mfrow = c(2, 4))
boxplot(data.gfs.ls$V4, main = "LS Command Performance for GFS", ylab = "Time in milliseconds")
boxplot(data.gfs.lsl$V4, main = "LSL Command Performance for GFS", ylab = "Time in milliseconds")
boxplot(data.gfs.cd$V4, main = "CD Command Performance for GFS", ylab = "Time in milliseconds")
boxplot(data.gfs.touch$V4, main = "TOUCH Command Performance for GFS", ylab = "Time in milliseconds")
boxplot(data.gfs.mkdir$V4, main = "MKDIR Command Performance for GFS", ylab = "Time in milliseconds")
boxplot(data.gfs.rmdir$V4, main = "RMDIR Command Performance for GFS", ylab = "Time in milliseconds")
boxplot(data.gfs.rmdirf$V4, main = "RMDIR FORCE Command Performance for GFS", ylab = "Time in milliseconds")
dev.off()

jpeg("NFS - Command wise performance.jpg", width = 1080, height = 480)
par(mfrow = c(2, 4))
boxplot(data.nfs.ls$V4, main = "LS Command Performance for NFS", ylab = "Time in milliseconds")
boxplot(data.nfs.lsl$V4, main = "LSL Command Performance for NFS", ylab = "Time in milliseconds")
boxplot(data.nfs.cd$V4, main = "CD Command Performance for NFS", ylab = "Time in milliseconds")
boxplot(data.nfs.touch$V4, main = "TOUCH Command Performance for NFS", ylab = "Time in milliseconds")
boxplot(data.nfs.mkdir$V4, main = "MKDIR Command Performance for NFS", ylab = "Time in milliseconds")
boxplot(data.nfs.rmdir$V4, main = "RMDIR Command Performance for NFS", ylab = "Time in milliseconds")
boxplot(data.nfs.rmdirf$V4, main = "RMDIR FORCE Command Performance for NFS", ylab = "Time in milliseconds")
dev.off()

jpeg("Ceph - Command wise performance.jpg", width = 1080, height = 480)
par(mfrow = c(2, 4))
boxplot(data.ceph.ls$V4, main = "LS Command Performance for Ceph", ylab = "Time in milliseconds")
boxplot(data.ceph.lsl$V4, main = "LSL Command Performance for Ceph", ylab = "Time in milliseconds")
boxplot(data.ceph.cd$V4, main = "CD Command Performance for Ceph", ylab = "Time in milliseconds")
boxplot(data.ceph.touch$V4, main = "TOUCH Command Performance for Ceph", ylab = "Time in milliseconds")
boxplot(data.ceph.mkdir$V4, main = "MKDIR Command Performance for Ceph", ylab = "Time in milliseconds")
boxplot(data.ceph.rmdir$V4, main = "RMDIR Command Performance for Ceph", ylab = "Time in milliseconds")
boxplot(data.ceph.rmdirf$V4, main = "RMDIR FORCE Command Performance for Ceph", ylab = "Time in milliseconds")
dev.off()

# Create boxplot for ceph comparison with partitions
jpeg("Ceph - With and without partitions.jpg", width = 1080, height = 480)
par(mfrow = c(1, 2))
boxplot(data.ceph.valid$V4, main = "Ceph with no replica and 1 MDS", ylab = "Time in milliseconds")
boxplot(data.ceph.replica.valid$V4, main = "Ceph with 2 MDS and 2 replica", ylab = "Time in milliseconds")
dev.off()

# Group by depths
data.gfs.ls.grouped = aggregate(V4 ~ V3, data.gfs.ls, mean)
data.gfs.lsl.grouped = aggregate(V4 ~ V3, data.gfs.lsl, mean)
data.gfs.mkdir.grouped = aggregate(V4 ~ V3, data.gfs.mkdir, mean)
data.gfs.touch.grouped = aggregate(V4 ~ V3, data.gfs.touch, mean)
data.gfs.cd.grouped = aggregate(V4 ~ V3, data.gfs.cd, mean)
data.gfs.rmdir.grouped = aggregate(V4 ~ V3, data.gfs.rmdir, mean)
data.gfs.rmdirf.grouped = aggregate(V4 ~ V3, data.gfs.rmdirf, mean)

data.nfs.ls.grouped = aggregate(V4 ~ V3, data.nfs.ls, mean)
data.nfs.lsl.grouped = aggregate(V4 ~ V3, data.nfs.lsl, mean)
data.nfs.mkdir.grouped = aggregate(V4 ~ V3, data.nfs.mkdir, mean)
data.nfs.touch.grouped = aggregate(V4 ~ V3, data.nfs.touch, mean)
data.nfs.cd.grouped = aggregate(V4 ~ V3, data.nfs.cd, mean)
data.nfs.rmdir.grouped = aggregate(V4 ~ V3, data.nfs.rmdir, mean)
data.nfs.rmdirf.grouped = aggregate(V4 ~ V3, data.nfs.rmdirf, mean)

data.ceph.ls.grouped = aggregate(V4 ~ V3, data.ceph.ls, mean)
data.ceph.lsl.grouped = aggregate(V4 ~ V3, data.ceph.lsl, mean)
data.ceph.mkdir.grouped = aggregate(V4 ~ V3, data.ceph.mkdir, mean)
data.ceph.touch.grouped = aggregate(V4 ~ V3, data.ceph.touch, mean)
data.ceph.cd.grouped = aggregate(V4 ~ V3, data.ceph.cd, mean)
data.ceph.rmdir.grouped = aggregate(V4 ~ V3, data.ceph.rmdir, mean)
data.ceph.rmdirf.grouped = aggregate(V4 ~ V3, data.ceph.rmdirf, mean)

# Create graph with level
jpeg("Performance with depth - LS.jpg", width = 1080, height = 480)
par(mfrow = c(1, 1))
plot(data.gfs.ls.grouped$V3, data.gfs.ls.grouped$V4, type = "n", xlim = c(3, 18), ylim = c(0, 150), main = "Performance for LS with different depth level", ylab = "Time in milliseconds", xlab = "Depth level")
lines(data.gfs.ls.grouped$V3, data.gfs.ls.grouped$V4, type = "l", col = "red")
lines(data.nfs.ls.grouped$V3, data.nfs.ls.grouped$V4, type = "l", col = "blue")
lines(data.ceph.ls.grouped$V3, data.ceph.ls.grouped$V4, type = "l", col = "green")
legend(3, 150, c("GFS", "NFS", "Ceph"), col = c("red", "blue", "green"), lty = c(1, 1, 1), lwd = c(2.5, 2.5, 2.5))
dev.off()

jpeg("Performance with depth - LSL.jpg", width = 1080, height = 480)
par(mfrow = c(1, 1))
plot(data.gfs.lsl.grouped$V3, data.gfs.lsl.grouped$V4, type = "n", xlim = c(3, 18), ylim = c(0, 120), main = "Performance for LSL with different depth level", ylab = "Time in milliseconds", xlab = "Depth level")
lines(data.gfs.lsl.grouped$V3, data.gfs.lsl.grouped$V4, type = "l", col = "red")
lines(data.nfs.lsl.grouped$V3, data.nfs.lsl.grouped$V4, type = "l", col = "blue")
lines(data.ceph.lsl.grouped$V3, data.ceph.lsl.grouped$V4, type = "l", col = "green")
legend(3, 120, c("GFS", "NFS", "Ceph"), col = c("red", "blue", "green"), lty = c(1, 1, 1), lwd = c(2.5, 2.5, 2.5))
dev.off()

jpeg("Performance with depth - CD.jpg", width = 1080, height = 480)
par(mfrow = c(1, 1))
plot(data.gfs.cd.grouped$V3, data.gfs.cd.grouped$V4, type = "n", xlim = c(3, 18), ylim = c(0, 150), main = "Performance for CD with different depth level", ylab = "Time in milliseconds", xlab = "Depth level")
lines(data.gfs.cd.grouped$V3, data.gfs.cd.grouped$V4, type = "l", col = "red")
lines(data.nfs.cd.grouped$V3, data.nfs.cd.grouped$V4, type = "l", col = "blue")
lines(data.ceph.cd.grouped$V3, data.ceph.cd.grouped$V4, type = "l", col = "green")
legend(3, 150, c("GFS", "NFS", "Ceph"), col = c("red", "blue", "green"), lty = c(1, 1, 1), lwd = c(2.5, 2.5, 2.5))
dev.off()

jpeg("Performance with depth - MKDIR.jpg", width = 1080, height = 480)
par(mfrow = c(1, 1))
plot(data.gfs.mkdir.grouped$V3, data.gfs.mkdir.grouped$V4, type = "n", xlim = c(4, 20), ylim = c(0, 100), main = "Performance for MKDIR with different depth level", ylab = "Time in milliseconds", xlab = "Depth level")
lines(data.gfs.mkdir.grouped$V3, data.gfs.mkdir.grouped$V4, type = "l", col = "red")
lines(data.nfs.mkdir.grouped$V3, data.nfs.mkdir.grouped$V4, type = "l", col = "blue")
lines(data.ceph.mkdir.grouped$V3, data.ceph.mkdir.grouped$V4, type = "l", col = "green")
legend(4, 100, c("GFS", "NFS", "Ceph"), col = c("red", "blue", "green"), lty = c(1, 1, 1), lwd = c(2.5, 2.5, 2.5))
dev.off()

jpeg("Performance with depth - TOUCH.jpg", width = 1080, height = 480)
par(mfrow = c(1, 1))
plot(data.gfs.touch.grouped$V3, data.gfs.touch.grouped$V4, type = "n", xlim = c(4, 18), ylim = c(0, 80), main = "Performance for TOUCH with different depth level", ylab = "Time in milliseconds", xlab = "Depth level")
lines(data.gfs.touch.grouped$V3, data.gfs.touch.grouped$V4, type = "l", col = "red")
lines(data.nfs.touch.grouped$V3, data.nfs.touch.grouped$V4, type = "l", col = "blue")
lines(data.ceph.touch.grouped$V3, data.ceph.touch.grouped$V4, type = "l", col = "green")
legend(4, 80, c("GFS", "NFS", "Ceph"), col = c("red", "blue", "green"), lty = c(1, 1, 1), lwd = c(2.5, 2.5, 2.5))
dev.off()

jpeg("Performance with depth - RMDIR.jpg", width = 1080, height = 480)
par(mfrow = c(1, 1))
plot(data.gfs.rmdir.grouped$V3, data.gfs.rmdir.grouped$V4, type = "n", xlim = c(3, 18), ylim = c(0, 100), main = "Performance for RMDIR with different depth level", ylab = "Time in milliseconds", xlab = "Depth level")
lines(data.gfs.rmdir.grouped$V3, data.gfs.rmdir.grouped$V4, type = "l", col = "red")
lines(data.nfs.rmdir.grouped$V3, data.nfs.rmdir.grouped$V4, type = "l", col = "blue")
lines(data.ceph.rmdir.grouped$V3, data.ceph.rmdir.grouped$V4, type = "l", col = "green")
legend(3, 100, c("GFS", "NFS", "Ceph"), col = c("red", "blue", "green"), lty = c(1, 1, 1), lwd = c(2.5, 2.5, 2.5))
dev.off()

jpeg("Performance with depth - RMDIRF.jpg", width = 1080, height = 480)
par(mfrow = c(1, 1))
plot(data.nfs.rmdirf.grouped$V3, data.nfs.rmdirf.grouped$V4, type = "n", xlim = c(3, 16), ylim = c(0, 90), main = "Performance for RMDIRF with different depth level", ylab = "Time in milliseconds", xlab = "Depth level")
lines(data.gfs.rmdirf.grouped$V3, data.gfs.rmdirf.grouped$V4, type = "l", col = "red")
lines(data.nfs.rmdirf.grouped$V3, data.nfs.rmdirf.grouped$V4, type = "l", col = "blue")
lines(data.ceph.rmdirf.grouped$V3, data.ceph.rmdirf.grouped$V4, type = "l", col = "green")
legend(17, 90, c("GFS", "NFS", "Ceph"), col = c("red", "blue", "green"), lty = c(1, 1, 1), lwd = c(2.5, 2.5, 2.5))
dev.off()