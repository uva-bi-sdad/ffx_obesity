## Libraries
library(readxl)
library(jsonlite)
library(data.table)
library(foreach)
library(doParallel)

## Functions
dmg_choice <- function(dt) {
    for (j in colnames(dt)) set(dt, j = j, value = dt[get(j) != "", .N, j][order(-N)][, ..j][1])
    dt[1]
}

dmg_choices <- function(dt, id_col) {
    dt[, dmg_choice(copy(.SD)), get(id_col)]
}

google_geocode <- function(address_str = "1600+Amphitheatre+Parkway,+Mountain+View,+CA",
                           key = "AIzaSyC9FKW-kjQlEXjfM3OgMZBJ7xE6zCN1JQI") {
    url <- sprintf("https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s",
                   address_str,
                   key)
    return <- jsonlite::fromJSON(URLencode(url))
    if (return$status[[1]]=="OK") {
        lat <- if(length(return$results$geometry$location$lat)==1) return$results$geometry$location$lat else NA
        lng <- if(length(return$results$geometry$location$lng)==1) return$results$geometry$location$lng else NA
        data.table::data.table(lat = lat, lng = lng)
    } else {
        data.table::data.table(lat = NA, lng = NA)
    }
}


## Geolocate Unique Program Locations
### load file
Program_Locations <- setDT(read_excel("data/ffx_obesity/original/park_service/FCPA registrations FY17 no names.xlsx", sheet = "LOCATIONS crosswalk"))
### prepare data
program_locations <- Program_Locations[, Address := gsub("Gov't", "Government", Address)]
program_locations[!is.na(City) & !is.na(St), full_address := sprintf("%s %s %s %s", Address, City, St, Zip)]
### geocode
program_locations[!is.na(full_address), coords := as.character(google_geocode(full_address)[, paste(lat, lng)]), full_address]
program_locations[!is.na(full_address), c("lat", "lng") := .(substr(coords, 1, regexpr(' ', coords)), substr(coords, regexpr(' ', coords) + 1, nchar(coords)))]
setnames(program_locations, "lat", "prgrm_lat")
setnames(program_locations, "lng", "prgrm_lng")
setnames(program_locations, "Location Code", "program_location")
### save results
write.csv(program_locations, "data/ffx_obesity/working/program_locations.csv", row.names = FALSE)


## Geolocate Unique Member Locations
### load files
FCPA_registrations_FY17_no_names <- setDT(read_excel("data/ffx_obesity/original/park_service/FCPA registrations FY17 no names.xlsx"))
FCPA_Pass_sales_and_usage_in_FY17_no_names <- setDT(read_excel("data/ffx_obesity/original/park_service/FCPA Pass sales and usage in FY17 no names.xlsx"))
Scholarship_Registrations_FY17 <- setDT(read_excel("data/ffx_obesity/original/park_service/Scholarship Registrations FY17.xlsx"))

### select and prepare data
registration_dmgs <- FCPA_registrations_FY17_no_names[, .(member_no = trimws(toupper(`Member#`)),
                                                          dob = as.character(dob),
                                                          address = trimws(toupper(address)),
                                                          city = trimws(toupper(city)),
                                                          st = trimws(toupper(st)),
                                                          zip = as.character(zip))]

pass_sales_dmgs <- FCPA_Pass_sales_and_usage_in_FY17_no_names[,.(member_no = trimws(toupper(`Member#`)),
                                                                 dob = as.character(`Date of Birth`),
                                                                 address = trimws(toupper(`Address`)),
                                                                 city = trimws(toupper(`City`)),
                                                                 st = trimws(toupper(`St`)),
                                                                 zip = as.character(`Zip`))]

scholarship_dmgs <- Scholarship_Registrations_FY17[,.(member_no = trimws(toupper(`Member#`)),
                                                      dob = as.character(`DOB`))]
### combine demographic datasets
combined_dmgs <- rbindlist(list(registration_dmgs, pass_sales_dmgs, scholarship_dmgs), fill = TRUE)
### deduplicate and choose unique addresses
unique_member_dmgs <- dmg_choices(combined_dmgs[, .(member_no, address, city, st, zip)], "member_no")
### save results
write.csv(unique_member_dmgs, "data/ffx_obesity/working/unique_member_dmgs.csv", row.names = FALSE)

### get unique_addresses from unique members
unique_member_dmgs <- fread("data/ffx_obesity/working/unique_member_dmgs.csv")
unique_member_addrs <- unique(unique_member_dmgs[, .(address, city, st, zip)])
### prepare data
unique_member_addrs[!is.na(city) & !is.na(st), full_address := sprintf("%s %s %s %s", address, city, st, zip)]
unique_member_addrs[full_address %like% "NEED NEW ADDRESS", full_address := NA]
unique_member_addrs[, id := seq(1:nrow(unique_member_addrs))]
# unique_member_addrs <- unique_member_addrs[1:100]
### geocode
myCluster<-makeCluster(16)
registerDoParallel(myCluster)
chunk <- 200
iterator <- idiv(nrow(unique_member_addrs), chunkSize=chunk)
startRow <- 1
endRow <- chunk

for (f in list.files("data/ffx_obesity/working/geo/", full.names = T)) {
    # unlink(f)
}

try(for (i in 1:ceiling(nrow(unique_member_addrs) / chunk)) {
    filename <-
        sprintf(
            "data/ffx_obesity/working/geo/geo_%s_%s.csv",
            startRow,
            endRow
        )
    if (!file.exists(filename)) {
        myChunkOfData <- unique_member_addrs[startRow:endRow]
        print(sprintf("Processing rows %s to %s", startRow, endRow))
        s <- foreach(r = iter(myChunkOfData, by = "row")) %dopar% {
            # print(r)
            # l <- list()

            w <- NULL
            attempt <- 1
            while( is.null(w) && attempt <= 3 ) {
                attempt <- attempt + 1
                try(
                    w <-  google_geocode(r$full_address[[1]])
                )
                if (is.null(colnames(w)))
                {
                    w <- c(V1=w[[1]])
                }
            }
            w$rownum <- r$rownum
            w$id <- r$id
            to.s <- w
        }

        t <- rbindlist(s, fill = TRUE)
        write.csv(t, filename)
    }
    startRow <- endRow + 1
    endRow <- endRow + nextElem(iterator)
}
, silent = FALSE
)
stopCluster(myCluster)

files_to_combine <- list.files("data/ffx_obesity/working/geo/", full.names = TRUE)
objs <- lapply(files_to_combine, function(x) {assign(basename(x), fread(x))})
combined <- rbindlist(objs, fill = TRUE)
combined <- combined[!is.na(id)]

setkey(combined, id)
setkey(unique_member_addrs, id)

unique_member_addrs_geolocated <- unique_member_addrs[combined, nomatch = 0]
unique_member_addrs_geolocated[is.na(full_address), c("lat", "lng") := .(NA, NA)]
unique_member_addrs_geolocated[, V1 := NULL]
unique_member_addrs_geolocated[, id := NULL]

# unique_member_addrs[!is.na(full_address), coords := as.character(google_geocode(full_address)[, paste(lat, lng)]), full_address]
# unique_member_addrs[!is.na(full_address), c("lat", "lng") := .(substr(coords, 1, regexpr(' ', coords)), substr(coords, regexpr(' ', coords) + 1, nchar(coords)))]
### save results
write.csv(unique_member_addrs_geolocated, "data/ffx_obesity/working/unique_member_addrs_geolocated.csv", row.names = FALSE)

setkeyv(unique_member_dmgs, c("address", "city", "st", "zip"))
setkeyv(unique_member_addrs_geolocated, c("address", "city", "st", "zip"))

unique_member_dmgs_geolocated <- unique_member_dmgs[unique_member_addrs_geolocated, nomatch = 0]
unique_member_dmgs_geolocated[, get := NULL]
unique_member_dmgs_geolocated[order(member_no)]
unique_member_dmgs_geolocated[, zip := as.numeric(zip)]

setkeyv(FCPA_registrations_FY17_no_names, c("Member#", "address", "city", "st", "zip"))
setkeyv(unique_member_dmgs_geolocated, c("member_no", "address", "city", "st", "zip"))
FCPA_registrations_memgeo <- unique_member_dmgs_geolocated[FCPA_registrations_FY17_no_names]
FCPA_registrations_memgeo[, full_address := NULL]
setnames(FCPA_registrations_memgeo, "lat", "member_lat")
setnames(FCPA_registrations_memgeo, "lng", "member_lng")
setnames(FCPA_registrations_memgeo, "Program Location", "program_location")

setkey(FCPA_registrations_memgeo, program_location)
setkey(program_locations, program_location)
FCPA_registrations_memgeo_prggeo <- program_locations[FCPA_registrations_memgeo]


FCPA_registrations_memgeo_prggeo_out <-
    FCPA_registrations_memgeo_prggeo[, .(
        member_mem = mem,
        member_asc = asc,
        member_no,
        member_address = address,
        member_city = city,
        member_st = st,
        member_zip = zip,
        member_lat,
        member_lng,
        member_dob = dob,
        member_gndr = gndr,
        member_reg_date = `reg date`,
        member_age = age,
        member_term = term,
        prgm_code = `pgm code`,
        prgm_no_mtgs = `#mtgs`,
        prgm_title = title,
        prgm_status = status,
        prgm_category = category,
        prgm_fee_paid = `Fee Paid`,
        prgm_loc_code = program_location,
        prgm_short_name = `Short name`,
        prgm_full_name = `Full Name`,
        prgm_addr = full_address,
        prgm_lat = prgrm_lat,
        prgm_lng = prgrm_lng
    )]

write.csv(FCPA_registrations_memgeo_prggeo_out, "data/ffx_obesity/working/FCPA_Registrations_Geocoded.csv", row.names = FALSE)
