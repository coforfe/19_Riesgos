#----------------------------
# Author:     Carlos Ortega
# Date:       2022-03-08
# Purpose:    Get Risks historical values.
# Input:      
# 1) Get Internal risk values.
# Output:     All files together + Some analysis (companies without margin)
#----------------------------


rm(list = ls())
tidytable::inv_gc()
# gc()
# cat("\014")  # ctrl+L

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(tictoc)
  library(stringr)
  library(stringi)
  library(lubridate)
  library(janitor)
  library(fasttime)
  library(forcats)
  library(readxl)
  library(broom)
  library(tictoc)
  library(tidytable)
  library(parallel)
  library(magrittr)
  library(rio)
})

tini <- Sys.time()
tic()

#--------- CONTRACTS IMPORT ------------------------
# I need to get CIFs, Provincia and add date for each file so I need to process them individually.
riskdir <- '/Users/carlosortega/Documents/00_Adecco/Para_J/01_Input_raw/Poliza_Interior/'
file_list <- list.files(path = riskdir, pattern = '*.xlsx')
file_list <- file_list[ !str_detect(file_list, "~")] 
file_list <- file_list[ !str_detect(file_list, "^2022")] 


#--- Function to read each file (all sheets)
read_excel_allsheets <- function(filename, tibble = FALSE) {
  # I prefer straight data.frames
  # but if you like tidyverse tibbles (the default with read_excel)
  # then just pass tibble = TRUE
  sheets <- readxl::excel_sheets(filename)
  x <- lapply(sheets, function(X) readxl::read_excel(filename, sheet = X, skip = 13))
  if (!tibble) x <- lapply(x, as.data.frame)
  names(x) <- sheets
  x
}

allrisk <- data.table()
for (i in 1:length(file_list)) {
  
  filetmp    <- paste(riskdir, file_list[i], sep = "")
  sheetstmp  <- read_excel_allsheets(filetmp)
  allrisktmp <- rbindlist(sheetstmp, idcol = TRUE)
  allrisk    <- rbind(allrisk, allrisktmp)
  
}