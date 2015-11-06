args <- commandArgs(trailingOnly = TRUE)

DEF_NUM_ARGS = 3

srcFilePath = ""
experimentName = ""

if (length(args) < DEF_NUM_ARGS) {
  print("Wrong number of arguments!")
  print("Usage:")
  print("RScript generate_plots.R <srcFilePath> <experimentName> <outputFolderPath>")
  stop()
} else {
  srcFilePath = args[1]
  experimentName = args[2]
  outputFolderPath = args[3]
}

a = read.table(srcFilePath,header=TRUE,sep=";")

a = a[,c("cluster_size","proc_time")]

png(paste(outputFolderPath,"/distribution-",experimentName,".png",sep=""))
plot(a,main=paste(experimentName,"Experiment - Feature Extraction algorithm"),xlab="Cluster size(#nodes)",ylab="Time (s)")
dev.off()

b = aggregate(proc_time ~ cluster_size, a, mean)

png(paste(outputFolderPath,"/mean-",experimentName,".png",sep=""))
plot(b,main=paste(experimentName,"Experiment - Feature Extraction algorithm"),xlab="Cluster size(#nodes)",ylab="Mean Time (s)")
dev.off()

png(paste(outputFolderPath,"/boxplots-",experimentName,".png",sep=""))
boxplot(proc_time ~ cluster_size,data=a, main=paste("Boxplots for",experimentName,"Experiment - Feature Extraction algorithm"),xlab="Cluster size(#nodes)",ylab="Mean Time (s)")
dev.off()
