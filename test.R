args <- commandArgs(trailingOnly = TRUE)
task_id <- args[length(args)]
message("This is R script running for task ", task_id, " on ", Sys.info()["nodename"])
