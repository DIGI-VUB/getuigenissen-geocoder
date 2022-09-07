library(data.table)
library(tools)
library(udpipe)
library(arrow)
x <- list.files("/var/www/brat/data/Getuigenissen/prod", recursive = TRUE, full.names = TRUE, pattern = ".ann")
#x <- list.files("/var/www/brat/data/Getuigenissen/Werkcollege2020", recursive = TRUE, full.names = TRUE, pattern = ".ann")
x <- file.info(x)
x <- as.data.frame(x, stringsAsFactors = FALSE)
x$file <- rownames(x)
x$basename <- basename(x$file)
x <- subset(x, size > 0)
x <- x[order(x$mtime, decreasing = FALSE), ]
x$minutes_to_next <- as.numeric(difftime(x$mtime, data.table::shift(x$mtime, n = 1, type = "lag"), units = "mins"))
x$creation_adjustment <- difftime(x$atime, x$ctime, units = "mins")
out <- x[, c("mtime", "basename", "minutes_to_next")]
rownames(out) <- NULL
table(as.Date(x$mtime))
table(as.Date(x$ctime))
table(as.Date(x$atime))

## Put annotations next to text
x$path <- sapply(x$file, file_path_sans_ext)
x$filename <- basename(x$path)
x$metadata_subject_id   <- as.integer(sapply(strsplit(x$filename, "-"), head, 1))
x$text <- sapply(sprintf("%s.txt", x$path), FUN=function(x) paste(readLines(x), collapse = "\n"))
x$anno <- lapply(sprintf("%s.ann", x$path), FUN=function(x){
    x <- readLines(x, encoding = "UTF-8")
    x <- strsplit(x, "\t")
    x <- data.frame(chunk_id = sapply(x, FUN=function(x) x[1]),
                    chunk = sapply(x, FUN=function(x) x[2]),
                    chunk_text = sapply(x, FUN=function(x) paste(x[-c(1, 2)], collapse = "\n")),
                    stringsAsFactors = FALSE)
    #x <- read.table(x, sep = "\t", header = FALSE, colClasses = "character", col.names = c("chunk_id", "chunk", "chunk_text"), stringsAsFactors = FALSE)
    x$chunk <- strsplit(x$chunk, " ")
    x$chunk_type_group <- txt_recode(substr(x$chunk_id, start = 1, stop = 1), from = c("T", "A", "E", "R"), to = c("Entiteit", "EntiteitsAttribuut", "Event", "Relatie"))
    x$chunk_type <- sapply(x$chunk, function(x) x[1])
    x$chunk_details <- sapply(x$chunk, function(x) paste(x[-1], collapse = " "))
    x[, c("chunk_id", "chunk_type_group", "chunk_type", "chunk_details", "chunk_text")]
})
x <- x[, c("metadata_subject_id", "filename", "text", "anno")]
x$anno <- lapply(x$anno, FUN=function(x){
    ## Parse entiteits informatie
    idx <- which(x$chunk_type_group == "Entiteit")
    information <- strsplit(x$chunk_details, split = " ")
    x$chunk_from <- rep(NA_integer_, nrow(x))
    x$chunk_to   <- rep(NA_integer_, nrow(x))
    if(length(idx) > 0){
        x$chunk_from[idx] <- as.integer(sapply(information, function(x) txt_collapse(head(x, 1)))[idx])
        x$chunk_to[idx]   <- as.integer(sapply(information, function(x) txt_collapse(tail(x, 1)))[idx])
    }
    ## Parse entiteits attributen
    idx <- which(x$chunk_type_group %in% "EntiteitsAttribuut")
    information <- strsplit(x$chunk_details, split = " ")
    x$chunk_attribute          <- rep(NA_character_, nrow(x))
    x$chunk_attribute_chunk_of <- rep(NA_character_, nrow(x))
    if(length(idx) > 0){
        x$chunk_attribute_chunk_of[idx] <- sapply(information, function(x) txt_collapse(head(x, 1)))[idx]
        x$chunk_attribute[idx]          <- sapply(information, function(x) txt_collapse(tail(x, 1)))[idx]
        i <- which(x$chunk_id %in% x$chunk_attribute_chunk_of[idx])
        x$chunk_attribute[i] <- txt_recode(x$chunk_id[i], from = rev(x$chunk_attribute_chunk_of[idx]), to = rev(x$chunk_attribute[idx])) ## give priority to latest input if double input
    }

    ## Parse relationships
    idx <- which(x$chunk_type_group %in% "Relatie")
    #x$chunk_relation      <- rep(NA_character_, nrow(x))
    x$chunk_links      <- rep(NA_character_, nrow(x))
    if(length(idx) > 0){
        #x$chunk_relation[idx] <- gsub(x$chunk_details[idx], pattern = "Arg[[:digit:]]+\\:", replacement = "")
        x$chunk_links[idx] <- gsub(x$chunk_details[idx], pattern = "Arg[[:digit:]]+\\:", replacement = "")
    }

    ## Parse events
    idx <- which(x$chunk_type_group %in% "Event")
    #x$chunk_events      <- rep(NA_character_, nrow(x))
    x$chunk_links_of      <- rep(NA_character_, nrow(x))
    if(length(idx) > 0){
        #x$chunk_events[idx] <- sapply(strsplit(x$chunk_details[idx], split = " "), FUN = function(x) txt_collapse(gsub(pattern = ".+\\:", replacement = "", x)))
        x$chunk_links[idx] <- sapply(strsplit(x$chunk_details[idx], split = " "), FUN = function(x) txt_collapse(gsub(pattern = ".+\\:", replacement = "", x)))
        x$chunk_links_of[idx] <- sapply(strsplit(x$chunk_type[idx], split = ":"), tail, 1)

        i <- which(x$chunk_id %in% x$chunk_links_of)
        if(length(i) > 0){
            x$chunk_details[i] <- txt_recode(x$chunk_id[i], from = x$chunk_links_of[idx], to = x$chunk_details[idx])
            x$chunk_links[i] <- txt_recode(x$chunk_id[i], from = x$chunk_links_of[idx], to = x$chunk_links[idx])
        }
    }
    x
})
x <- setDT(x)
x <- x[, unlist(.SD$anno, recursive = FALSE), by = list(metadata_subject_id, filename, text)]
#replacer      <- subset(x, chunk_type_group == "Event", select = c("metadata_subject_id", "filename", "chunk_id", "chunk_type"))
#replacer$from <- replacer$chunk_id
#replacer$to   <- sapply(strsplit(replacer$chunk_type, split = ":"), tail, n = 1)


recode_chunkids <- function(x, from, to){
    if(is.na(x)) return(x)
    x <- strsplit(x, " ")
    x <- unlist(x)
    x <- txt_recode(x, from = from, to = to)
    x <- paste(x, collapse = " ")
    x
}
recode_chunkdetails <- function(x, type = "chunk_details"){
    replacer      <- subset(x, chunk_type_group == "Event", select = c("chunk_id", "chunk_type"))
    replacer$from <- replacer$chunk_id
    replacer$to   <- sapply(strsplit(replacer$chunk_type, split = ":"), tail, n = 1)

    if(type == "chunk_details"){
        x$test <- strsplit(x$chunk_details, split = " ")
        x$test <- sapply(x$test, FUN = function(x){
            x <- strsplit(x, split = ":")
            x <- sapply(x, FUN = function(x){
                x <- txt_recode(x, from = replacer$from, to = replacer$to)
                paste(x, collapse = ":")
            })
            paste(x, collapse = " ")
        })
    }else{
        x$test <- sapply(x[[type]], FUN = function(x, from, to) recode_chunkids(x, from = from, to = to), from = replacer$from, to = replacer$to, USE.NAMES = FALSE)
    }
    x$test
}
x <- x[, chunk_attribute_chunk_of := recode_chunkdetails(.SD, type = "chunk_attribute_chunk_of"), by = list(metadata_subject_id, filename)]
x <- x[, chunk_links              := recode_chunkdetails(.SD, type = "chunk_links"), by = list(metadata_subject_id, filename)]
x <- x[, chunk_links_of           := recode_chunkdetails(.SD, type = "chunk_links_of"), by = list(metadata_subject_id, filename)]
x <- x[, chunk_details            := recode_chunkdetails(.SD, type = "chunk_details"), by = list(metadata_subject_id, filename)]
annotations <- x
annotations <- subset(annotations, !chunk_type_group %in% c("EntiteitsAttribuut", "Event"),
    select = c("metadata_subject_id", "filename", "text",
    "chunk_text", "chunk_id", "chunk_type_group", "chunk_type", "chunk_attribute",
    "chunk_from", "chunk_to", "chunk_links", "chunk_details"))
table(annotations$filename)
annotations <- setDF(annotations)
saveRDS(annotations, file = "annotations.rds")
save(annotations, file = "annotations.RData")
write.csv(annotations, file = "annotations.csv", row.names = FALSE, na = "")
write_parquet(annotations, "annotations.parquet")
annotations$text <- NULL
annotations$link <- sprintf("https://brat.datatailor.be/#/Getuigenissen/Werkcollege2020/%s/%s",
sapply(strsplit(basename(annotations$filename), "-"), FUN=function(x) paste(x[-1], collapse = "-")),
basename(annotations$filename))
writexl::write_xlsx(annotations, "annotations.xlsx")

# T: entiteit
# A: attribuut van entiteit
# E: event (misdrijf / ... event type (misdrijf / activiteit))
# R: relatie tussen entiteiten (leeftijd_wie)

##
## Extract geolocations in order to do geocoding
##
#x <- subset(annotations, chunk_type %in% c("PLAATS-GEBEURTENIS-PRIVAAT", "PLAATS-GEBEURTENIS-PUBLIEK", "PLAATS-GEBEURTENIS-SEMI-PUBLIEK", "GEO-ADRES", "GEO-PLAATS-GEBEURTENIS"))
#writexl::write_xlsx(x, path = "entiteit-plaatsen.xlsx")

## Show progress
x <- annotations[, list(aantal = .N), by = list(link)]
x <- x[order(x$aantal, decreasing = TRUE), ]
x
writexl::write_xlsx(x, "progress_20201123.xlsx")
