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


#---- Put all files together.
tic()
allrisk <- data.frame()
for (i in 1:length(file_list)) {
  
  print(file_list[i])
  filetmp    <- paste(riskdir, file_list[i], sep = "")
  sheetstmp  <- readxl::excel_sheets(filetmp) 
  
  # there are inconsistencies in names between sheets. Missing column in 2019 and part of
  # 2020 colum "Fecha anulación". Check if it exists, if not add it, since now it is the default. 
  filepile <- data.frame() 
  for (j in 1:length(sheetstmp)) {
    
    print(c(i, j, sheetstmp[j]))
    tmpsheet <- readxl::read_excel(filetmp, sheet = sheetstmp[j], skip = 13) %>%
      as.data.frame() %>%
      clean_names()
    namtmp   <- names(tmpsheet)
    # print(namtmp)
    
    if ( !("fecha_anulacion" %in% namtmp) ) {
      tmpsheet %<>%
        mutate( fecha_anulacion = 0) %>%
        relocate.( fecha_anulacion, .before = localidad) %>%
        mutate.( midate = stri_trans_tolower(sheetstmp[j])) %>%
        as.data.frame()
      
      # namend <- names(tmpsheet)
        filepile <- rbind(filepile, tmpsheet)
        # print(namend)
    } else {
        tmpsheet %<>%
           mutate.( midate = stri_trans_tolower(sheetstmp[j])) %>%
           as.data.frame() %>% 
           clean_names()
      
        filepile <- rbind(filepile, tmpsheet)
        # print(namend)
    }
  }
  
  allrisk    <- rbind(allrisk, filepile)
  
}
toc()

#- Clean environment
rm(filepile, tmpsheet, i, j, namtmp, sheetstmp, filetmp, file_list, riskdir)
tend <- Sys.time(); tend - tini
# Time difference of 27.17682 secs
