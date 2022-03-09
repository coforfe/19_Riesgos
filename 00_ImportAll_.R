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

#--- Transform and create new variables realted to "fecha" -----------
allrisk %<>%
  mutate.( midatetr =  stri_trans_totitle(midate)) %>%
  mutate.( mes      = stri_extract_first_words(midatetr)) %>%
  mutate.( ano      = as.numeric(stri_extract_last_words(midatetr)) ) %>%
  as.data.table()

allrisk[ , mesnum := fcase(
  mes == "Enero", 1,
  mes == "Febrero", 2,
  mes == "Marzo", 3,
  mes == "Abril", 4,
  mes == "Mayo", 5,
  mes == "Junio", 6,
  mes == "Julio", 7,
  mes == "Agosto", 8,
  mes == "Septiembre", 9,
  mes == "Octubre", 10,
  mes == "Noviembre", 11, 
  mes == "Diciembre", 12
)
]

allrisk %<>%
  mutate.( yearmon = (ano * 100 + mesnum ) ) %>% 
  mutate.( yearmondat = ym(yearmon ) ) %>%
  # sort by yearmon I will need later.
  arrange.(yearmon) %>%
  as.data.table()

tend <- Sys.time(); tend - tini

#---- New variables related to "importe_concedido" -------
last1_val <- rev(unique(allrisk$yearmon)) %>% head(., 1) %>% tail(., 1)
last3_val <- rev(unique(allrisk$yearmon)) %>% head(., 3) %>% tail(., 1)
last6_val <- rev(unique(allrisk$yearmon)) %>% head(., 6) %>% tail(., 1)

#--- Calculate if the last 1, 3, 6 months customer had "0" in "importe_concedido".
allrisk %<>%
  mutate.( withrisk     = ifelse.(importe_concedido == 0, 1, 0)) %>%
  #--- 1 month
  mutate.( last1mon     = ifelse.(yearmon >= last1_val, 1, 0)) %>%
  mutate.( risk1mon     = ifelse.(withrisk == 1 & last1mon == 1, 1, 0)) %>%
  mutate.( hwrisk1mon   = sum(risk1mon), .by = c(razon_social, nif_cif)) %>%
  #--- 3 month
  mutate.( last3mon     = ifelse.(yearmon >= last3_val, 1, 0)) %>%
  mutate.( risk3mon     = ifelse.(withrisk == 1 & last3mon == 1, 1, 0)) %>%
  mutate.( hwrisk3mon   = sum(risk3mon), .by = c(razon_social, nif_cif)) %>%
  #--- 6 month
  mutate.( last6mon     = ifelse.(yearmon >= last6_val, 1, 0)) %>%
  mutate.( risk6mon     = ifelse.(withrisk == 1 & last6mon == 1, 1, 0)) %>%
  mutate.( hwrisk6mon   = sum(risk6mon), .by = c(razon_social, nif_cif)) %>%
  #--- Synthetic variable paste of the risks.
  mutate.( riskcomb     = paste(hwrisk1mon, hwrisk3mon, hwrisk6mon, sep = "_")) %>% 
  as.data.table()


tend <- Sys.time(); tend - tini
# Time difference of 30.26061 secs

#---- Calculate customers with different levels of risk
custrisk <- allrisk %>%
  select.(razon_social, nif_cif, provincia, hwrisk1mon, hwrisk3mon, hwrisk6mon, riskcomb) %>%
  arrange.(nif_cif) %>%
  distinct.() %>%
  as.data.table()

#---- Summary with how many in risk we have based on "riskcomb" variable.
custrisk %>%
  count.(riskcomb, sort = TRUE) %>%
  mutate.( total = sum(N)) %>%
  mutate.( perc  = round(N * 100 / total, 2)) %>%
  as.data.table()

#-- Result
#    riskcomb     N total  perc
# 1:    0_0_0 82455 86653 95.16
# 2:    1_3_6   905 86653  1.04
# 3:    0_0_1   446 86653  0.51
# 4:    1_1_1   387 86653  0.45
# 5:    0_1_4   334 86653  0.39
# 6:    1_3_4   330 86653  0.38
# 7:    0_2_5   310 86653  0.36
# 8:    0_0_2   300 86653  0.35
# 9:    0_0_3   297 86653  0.34
# 10:   1_3_5   287 86653  0.33

#---- Which are the customers with the pattern "1_3_6" -> High risk last 6 months.
empresasrisk <- custrisk %>%
  filter.( riskcomb == "1_3_6") %>%
  select.( razon_social, nif_cif, provincia, riskcomb) %>%
  mutate.( hwprovin = n.(), .by = provincia) %>%
  mutate.( total = n.()) %>%
  mutate.( perprov = round(hwprovin * 100 / total, 2)) %>%
  arrange.( -perprov) %>%
  as.data.table()
empresasrisk

#--- Save file ------
fwrite(
  empresasrisk,
  file = "./output/EmpresasRisk_1_3_6.csv",
  sep = "|", 
  bom = TRUE,
  encoding = "UTF-8",
  dec = ","
)

empresasrisk %>%
  select.(provincia, perprov) %>%
  distinct.() %>%
  as.data.table()

#----- END OF FILE --------
tend <- Sys.time(); tend - tini
# Time difference of 30.4016 secs