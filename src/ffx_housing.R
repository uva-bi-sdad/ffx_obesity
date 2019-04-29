# join Fairfax housing data from the portal

# links to data:
# https://data-fairfaxcountygis.opendata.arcgis.com/datasets/current-housing-units
# https://data-fairfaxcountygis.opendata.arcgis.com/datasets/tax-administrations-real-estate-dwelling-data
# https://data-fairfaxcountygis.opendata.arcgis.com/datasets/tax-administrations-real-estate-assessed-values
# https://data-fairfaxcountygis.opendata.arcgis.com/datasets/tax-administrations-real-estate-land-data
# https://data-fairfaxcountygis.opendata.arcgis.com/datasets/tax-administrations-real-estate-parcels-data

# links to metadata:
# https://www.fairfaxcounty.gov/maps/sites/maps/files/assets/documents/dta_assessment_tables.pdf
# https://www.fairfaxcounty.gov/demographics/sites/demographics/files/assets/datadictionary/ipls-data-dictionary-gis.pdf


library(rgdal)
library(dplyr)

setwd("~/git/ffx_obesity/src/")

housing_units <- readOGR(dsn="~/../sdad/project_data/ffx/comm_fairfax/housing_stock_2018/Current_Housing_Units/",layer="Current_Housing_Units")

# Metadata (from IPLS data dictionary):
# PIN         parcel identification number
# PARCE_ID    parcel ID
# CURRE_UNIT  current housing unit count
# HOUSI_UNIT  housing unit type
# SF: single family detached
# TH: townhouse
# DX: duplex
# MP: multiplex
# LR: low rise
# MR: mid rise
# HR: high rise
# MH: mobile home
# LUC         land use code [see IPLS data dictionary]
# STRUC_DESC  structure description; 99.9% missing
# YEAR_BUILT  year built (451 parcels are '0', two parcels are '2100')
# VALID_FROM  missing
# VALID_TO    'upper validity date'; all '2018-01-01'

# Table can contain multiple records for a PIN in a single year due to year built or housing unit type.
# Three fields must be combined to create a unique identifier – PIN, YEAR_BUILT, and HOUSI_UNIT
# "Joining household and population or forecast tables to this table is not straightforward.
# This is because some tables contain pins that are not in other tables and some tables contain multiple records for a single pin."

# -----------------------------------------

# now read in tax assessment data and join all by parcel ID
tax_parcel <- read.csv("~/../sdad/project_data/ffx/comm_fairfax/housing_stock_2018/Tax_Administration's_Real_Estate___Parcels_Data.csv")
tax_assessment <- read.csv("~/../sdad/project_data/ffx/comm_fairfax/housing_stock_2018/Tax_Administration's_Real_Estate__Assessed_Values.csv")
tax_dwelling <- read.csv("~/../sdad/project_data/ffx/comm_fairfax/housing_stock_2018/Tax_Administration's_Real_Estate__Dwelling_Data.csv")
tax_land <- read.csv("~/../sdad/project_data/ffx/comm_fairfax/housing_stock_2018/Tax_Administration's_Real_Estate__Land_Data.csv")
#tax_legal <- read.csv("~/../sdad/project_data/ffx/comm_fairfax/housing_stock_2018/Tax_Administration's_Real_Estate__Legal_Data.csv")

# -----------------------------------------
# Variables to include in each table:

# Housing units: Parcel ID (PIN/PARID), Num Units, Housing Type, Year Built
# Parcel: Water, Sewer, Gas
# Assessment: Property Value (land + buildings; entire parcel)
# Dwelling: Bedroom count, Bathroom count (excluding half baths), Living Area, Year Built
# Land: Land Area (sqft,acres), Num Units

# Steps for cleaning/joining the data at the *building* level:

# 1. join lat and long to housing units data; clean year built
# 2. left join parcel by parcel ID (1 to many; duplicates for water, sewer, gas)
# 3. left join tax_assessment by parcel ID (1 to many; duplicates for property value [total value of the parcel])
# 4. left join tax_dwelling by matching parcel ID and year built
# 5. join to tax_land to get land area (how? note that LLINE is the 'line number'; unique ID within each parcel)

# -----------------------------------------

# get lat and long of each building
coords <- data.frame(housing_units@coords); names(coords) <- c("LONGITUDE","LATITUDE")

# select and rename import info, add a building ID
ffx_housing <- housing_units@data %>% dplyr::select(PARCEL_ID=PIN,NUM_UNITS=CURRE_UNIT,HOUSING_TYPE=HOUSI_UNIT,YEAR_BUILT)
ffx_housing$LONGITUDE <- coords$LONGITUDE
ffx_housing$LATITUDE <- coords$LATITUDE
ffx_housing <- cbind(BUILDING_ID=1:nrow(ffx_housing),ffx_housing)

# remove 'factors'
ffx_housing$PARCEL_ID <- paste(ffx_housing$PARCEL_ID)
ffx_housing$NUM_UNITS <- as.numeric(paste(ffx_housing$NUM_UNITS))
ffx_housing$HOUSING_TYPE <- paste(ffx_housing$HOUSING_TYPE)
ffx_housing$YEAR_BUILT <- as.numeric(paste(ffx_housing$YEAR_BUILT))

# use long names for housing type
ffx_housing$HOUSING_TYPE[ffx_housing$HOUSING_TYPE=="SF"] <- "Single Family Detached"
ffx_housing$HOUSING_TYPE[ffx_housing$HOUSING_TYPE=="TH"] <- "Townhouse"
ffx_housing$HOUSING_TYPE[ffx_housing$HOUSING_TYPE=="DX"] <- "Duplex"
ffx_housing$HOUSING_TYPE[ffx_housing$HOUSING_TYPE=="MP"] <- "Multiplex"
ffx_housing$HOUSING_TYPE[ffx_housing$HOUSING_TYPE=="LR"] <- "Low Rise"
ffx_housing$HOUSING_TYPE[ffx_housing$HOUSING_TYPE=="MR"] <- "Mid Rise"
ffx_housing$HOUSING_TYPE[ffx_housing$HOUSING_TYPE=="HR"] <- "High Rise"
ffx_housing$HOUSING_TYPE[ffx_housing$HOUSING_TYPE=="MH"] <- "Mobile Home"

# set invalid values for year built to missing
ffx_housing$YEAR_BUILT[ffx_housing$YEAR_BUILT==0] <- NA
ffx_housing$YEAR_BUILT[ffx_housing$YEAR_BUILT==2100] <- NA

# -----------------------------------------

# 2. left join parcel data by parcel ID (water, sewer, gas)

# set blank descriptions to missing
tax_parcel$UTIL1_DESC[tax_parcel$UTIL1_DESC==""] <- NA
tax_parcel$UTIL2_DESC[tax_parcel$UTIL2_DESC==""] <- NA
tax_parcel$UTIL3_DESC[tax_parcel$UTIL3_DESC==""] <- NA

# deduplicate by parcel
tax_parcel_dedup <- tax_parcel %>%
    group_by(PARID) %>% slice(1) %>% ungroup

# join
ffx_housing2 <- ffx_housing %>%
    left_join(
        tax_parcel_dedup %>% dplyr::select(PARCEL_ID=PARID, WATER=UTIL1_DESC, SEWER=UTIL2_DESC, GAS=UTIL3_DESC),
        by = "PARCEL_ID"
    )

# -----------------------------------------

# 3. left join tax_assessment by parcel ID (1 to many; duplicates for property value [total value of the parcel])
ffx_housing3 <- ffx_housing2 %>%
    left_join(
        tax_assessment %>% dplyr::select(PARCEL_ID=PARID, VALUE_LAND=APRLAND, VALUE_BUILDING=APRBLDG, VALUE_TOTAL=APRTOT),
        by = "PARCEL_ID"
    )
# APRLAND         final appraised value for land
# APRBLDG         final appraised value for building
# APRTOT          final appraised value total

# -----------------------------------------

# 4. left join tax_dwelling by matching parcel ID and year built
tax_dwelling2 <- tax_dwelling %>%
    dplyr::select(PARCEL_ID=PARID, YEAR_BUILT=YRBLT, BEDROOMS=RMBED, BATHROOMS=FIXBATH, LIVING_AREA=SFLA)

# remove 226 duplicates by parcel ID and year built
tax_dwelling_dedup <- tax_dwelling2 %>%
    group_by(PARCEL_ID, YEAR_BUILT) %>% slice(1) %>% ungroup

ffx_housing4 <- ffx_housing3 %>%
    left_join(tax_dwelling_dedup,by = c("PARCEL_ID","YEAR_BUILT"))

# -----------------------------------------

# 5. join to tax_land to get land area (how? note that LLINE is the 'line number'; unique ID within each parcel)

# -----------------------------------------

# save the final joined data
write.csv(ffx_housing4, file="~/../sdad/project_data/ffx/comm_fairfax/housing_stock_2018/fairfax_housing_2018.csv")

# -----------------------------------------

# Metadata, notes for the other tables:

names(tax_parcel)
# metadata (from dta assessment tables)
# https://www.fairfaxcounty.gov/taxes/real-estate
# PARID           parcel ID -- 1 to many; why?
# TAXYR           tax year (all 2020)
# LIVUNIT         livable units
# LOCATION_DESC   Site description
# STREET1_DESC    Street description
# LUC_DESC        Land use type description
# UTIL1_DESC      Water utility description
# UTIL2_DESC      Sewer utility description
# UTIL3_DESC      Gas utility description
# ZONING_DESC     Assessment zoning description

# note: 2,218 parcels in the (2018) housing unit data are not in 2019 tax data; these are the new units

# look at duplicates by parcel ID:
dupes_parcel <-  tax_parcel[duplicated(tax_parcel$PARID) | duplicated(tax_parcel$PARID,fromLast=TRUE),]


# -----------------------------------------

names(tax_assessment)
# metadata (from DTA assessment tables)
# PARID           parcel ID -- 1 to 1
# TAXYR           tax year (all 2019)
# APRLAND         final appraised value for land
# APRBLDG         final appraised value for building
# APRTOT          final appraised value total
# PRILAND         prior year appraised value for land
# PRIBLDG         prior year appraised value for building
# PRITOT          prior year appraised value total
# FLAG4_DESC      tax exempt code description

# -----------------------------------------

names(tax_land)
# metadata (from dta assessment tables)
# PARID           parcel ID -- 1 to many, careful!
# TAXYR           tax year (all 2020)
# LLINE           line number (what is this?)
# SF              land size (square ft)
# ACRES           land size (acres)
# UNITS           number of units
# CODE_DESC       land description [includes commercial, hospital, hotel, industrial, warehouse, etc.]

# filter out commercial properties?
#resid_land <- tax_land %>% filter(CODE_DESC %in% c("DUPLEX","HOMESITE","MULTIFAMILY/ELDERLY","RESIDENTIAL","SINGLE DWELLING",
#                                                   "SINGLE DWELLING RESIDUAL","TWNHSE END","TWNHSE INT"))

# -----------------------------------------

names(tax_dwelling)
# metadata (from dta assessment tables)
# PARID           parcel ID (joins w/ PIN) -- 1 to many, careful!
# STYLE_DESC      style (#stories)
# YRBLT           year built
# EFFYR           effective year built from remodel
# YRREMOD         year house remodeled
# RMBED           bedroom count
# FIXBATH         full bath count
# FIXHALF         half bath count
# RECROMAREA      basement rec rom size (square ft)
# WBFP_PF         number of fireplaces
# BSMTCAR         basement garage (# cars)
# USER6           model name
# USER10          basement bedrooms/dens count
# GRADE_DESC      construction quality/grade
# SFLA            above grade living area total sq ft
# BSMT_DESC       basement description
# CDU_DESC        physical condition description
# EXTWALL_DESC    Exterior wall material
# HEAT_DESC       Heating description
# USER13_DESC     Roof type description
# USER7_DESC      Dormer type description
# USER9_DESC      Basement type description
# CreationDate    Creation Date (all 2019-04-14)


