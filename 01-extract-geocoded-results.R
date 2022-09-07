library(sf)
library(leaflet)
library(sp)
library(RPostgreSQL)
library(tools)
library(magrittr)
library(udpipe)
library(data.table)
library(stringdist)
##############################################################################
## Get all the shapes used in the project
##
maps <- readRDS("/home/jwijffels/getuigenissen/dashdata_maps.rds")

##############################################################################
## Read the annotations
##
DBconnection <- function(user = c("digivub", "qgis")){
  user <- match.arg(user)
  library(RPostgreSQL)
  con <- dbConnect(drv = PostgreSQL(),
                   user = Sys.getenv("GETUIGENISSEN_GEOCODING_USER"),
                   dbname = Sys.getenv("GETUIGENISSEN_GEOCODING_DB"),
                   password = Sys.getenv("GETUIGENISSEN_GEOCODING_PWD"),
                   host = Sys.getenv("GETUIGENISSEN_GEOCODING_HOST"),
                   port = Sys.getenv("GETUIGENISSEN_GEOCODING_PORT"))
  con
}
con <- DBconnection()
x <- st_read(con, layer = c("getuigenissen", "locations_geocoded"), as_tibble = FALSE)
x <- subset(x, modus %in% "prod")
x$datasource <- ifelse(x$comment %in% "gazetteer", x$comment, x$datasource)
x <- subset(x, datasource %in% c("geo.rds", "geo_stedenatlas.rds", "gazetteer"))
x <- x[, c("annotation_time", "doc_id", "text", "chunk_id", "chunk_text",
           "type", "shape_id", "label", "radius", "spatial_map",
           "spatial_id", "spatial_label", "comment", "datasource", "is_generieke_locatie",
           "geometry")]
x$type <- ifelse(x$type %in% "zelfgedefinieerd-spatiaal-object" & x$comment %in% "onmogelijk te geocoderen",
                 "onmogelijk-te-geocoderen", x$type)
x <- x[, c("annotation_time", "doc_id", "text", "chunk_id", "chunk_text",
           "type", "datasource", "shape_id", "label", "radius", "spatial_map",
           "spatial_id", "spatial_label", "is_generieke_locatie",
           "geometry")]
dbDisconnect(con)

##############################################################################
## Gekende objecten: voeg er het bestaande aan toe
##
idx               <- which(x$type %in% "gekend-spatiaal-object")
geoms             <- data.frame(shape_id = x$shape_id[idx],
                                spatial_map = x$spatial_map[idx], stringsAsFactors = FALSE)
geoms$spatial_map2 <- sapply(strsplit(geoms$shape_id, split = "::"), head, n = 1)
geoms$spatial_id   <- sapply(strsplit(geoms$shape_id, split = "::"), tail, n = 1)

for(i in seq_along(idx)){
  #for(i in 1:200){
  idx_i       <- idx[i]
  id          <- geoms$shape_id[i]
  spatial_map <- geoms$spatial_map[i]
  area        <- maps[[spatial_map]]
  if(inherits(area, "Spatial")){
    area              <- subset(area, shape_id %in% id)
    if(nrow(area) > 1){
      if(inherits(area, "SpatialPolygonsDataFrame")){
        area <- gUnaryUnion(area)
      }else{
        area <- gLineMerge(area)
      }
    }
    area              <- st_as_sf(area)
    x$geometry[idx_i] <- area$geometry
  }
}
x$geometry_empty <- st_is_empty(x$geometry)
x$geometry_type  <- ifelse(!x$geometry_empty, as.character(st_geometry_type(x$geometry)), NA_character_)

table(x$geometry_empty)
table(x$datasource, x$geometry_empty, exclude = c())
table(x$spatial_map, x$geometry_empty, exclude = c())
table(x$type, x$geometry_empty, exclude = c())

x <- x[, c("annotation_time", "doc_id", "text", "chunk_id", "chunk_text",
           "type", "datasource", "label", "geometry_type", "geometry", "radius",
           "spatial_map", "spatial_label", "shape_id",
           "is_generieke_locatie")]
x <- unique(x)
x <- x[order(x$annotation_time, decreasing = TRUE), ]
x <- x[!duplicated(x[, c("datasource", "doc_id", "chunk_id", "chunk_text")]), ]

length(unique(paste(x$doc_id, x$chunk_id, sep = "-")[x$datasource %in% "geo.rds"]))

########################################################################v
## Text chunks al getagd met generieke locatie: haal de generieke locatie eruit + voeg de spatiale informatie toe
##
generieke_locaties <- subset(x, is_generieke_locatie == TRUE)
generieke_locaties <- generieke_locaties[order(generieke_locaties$annotation_time, decreasing = FALSE), ]
fields             <- c("annotation_time", "chunk_text",
                        "type", "datasource", "label", "geometry_type", "geometry", "radius",
                        "spatial_map", "spatial_label", "shape_id", "is_generieke_locatie")
dim(generieke_locaties)
dim(unique(generieke_locaties))
generieke_locaties <- generieke_locaties[!duplicated(generieke_locaties[, setdiff(fields, "annotation_time")]), fields]
dim(unique(generieke_locaties))
generieke_locaties <- unique(generieke_locaties)
table(generieke_locaties$type, generieke_locaties$datasource)


uploads <- list()
uploads[["geo_stedenatlas.rds"]] <- readRDS("/home/jwijffels/getuigenissen/uploaded/geo_stedenatlas.rds")
uploads[["geo.rds"]]             <- readRDS("/home/jwijffels/getuigenissen/uploaded/geo.rds")
dim(uploads[["geo.rds"]])
length(unique(paste(uploads$geo.rds$doc_id, uploads$geo.rds$chunk_id, sep = "-")))
uploads <- lapply(uploads, FUN = function(x){
  x$chunk_entity <- NULL
  x
})
uploads[["geo.rds"]]             <- merge(uploads[["geo.rds"]],
                                          subset(generieke_locaties, datasource %in% c("geo.rds", "gazetteer")),
                                          by = "chunk_text", all.x = TRUE, all.y = FALSE, sort = FALSE)
length(unique(paste(uploads$geo.rds$doc_id, uploads$geo.rds$chunk_id, sep = "-")))
missings <- uploads[["geo.rds"]]
missings <- missings[order(factor(missings$datasource, levels = c("geo.rds", "gazetteer")), decreasing = FALSE), ]
missings <- missings[!duplicated(paste(missings$doc_id, missings$chunk_id, sep = "-")), ]
dim(missings)
missings <- subset(missings, !paste(doc_id, chunk_id, sep = "-") %in% paste(x$doc_id, x$chunk_id, sep = "-"))
table(missings$type)
table(missings$datasource)
missings$datasource <- "geo.rds"
setdiff(colnames(missings), colnames(x))
setdiff(colnames(x), colnames(missings))
missings <- missings[, colnames(x)]
missings <- st_sf(missings, sf_column_name = "geometry")
plot(missings[, c("geometry", "geometry_type")])

View(subset(missings, is.na(geometry_type)))
table(missings$geometry_type, exclude = c())

#missings    <- uploads[["geo.rds"]]
#missings$id <- paste(missings$doc_id, missings$chunk_id, sep = "-")
#missings    <- subset(missings, id %in% missings$id[which(duplicated(missings$id))])
#sum(duplicated(paste(missings$doc_id, missings$chunk_id)))
table(x$datasource)

results <- rbind(x, missings)
results <- results[, c("datasource", setdiff(colnames(results), "datasource"))]

##############################################################################
## Postprocessing - data checks
##
table(results$datasource)
test    <- subset(results, datasource %in% "geo.rds")
test$id <- paste(test$doc_id, test$chunk_id, sep = "-")
View(subset(test, id %in% test$id[duplicated(test$id)]))
# should have: 3394

results$geometry_type <- as.character(st_geometry_type(results$geometry))

if(FALSE){
  con <- DBconnection()
  dbWriteTable(con, name = c("getuigenissen", "getuigenissen_brugge_geocoded"), value = results, append = FALSE)
  dbDisconnect(con)
  setwd("/home/jwijffels/getuigenissen/shp")
  saveRDS(results, "getuigenissen_brugge_geocoded.rds")
}

##############################################################################
## Postprocessing - add shape_type
##
lastelement <- function(x){
  sapply(strsplit(x, "::"), tail, n = 1)
}
results$shape_type <- NA_character_
results$shape_type <- ifelse(results$spatial_map %in% "popp",
                             txt_recode(lastelement(results$shape_id), from = lastelement(maps$popp$shape_id), to = maps$popp$shape_type,
                                        na.rm = TRUE), results$shape_type)
results$shape_type <- ifelse(results$spatial_map %in% "straten_17de_eeuw",
                             txt_recode(lastelement(results$shape_id), from = lastelement(maps$straten_17de_eeuw$shape_id), to = maps$straten_17de_eeuw$shape_type,
                                        na.rm = TRUE), results$shape_type)
table(results$shape_type, results$spatial_map, exclude = c())


##############################################################################
## Postprocessing - convert polygons to points
##
results$geometry_type <- as.character(st_geometry_type(results$geometry))
table(results$geometry_type, results$datasource)
table(results$spatial_map, results$geometry_type)

## Stedenatlas: center of polygon
idx  <- which(results$geometry_type %in% c("POLYGON", "MULTIPOLYGON") & results$datasource %in% "geo_stedenatlas.rds")
test <- results[idx, ]
table(test$geometry_type, test$datasource)
st_crs(test)
converted_to_points <- st_transform(test, crs = 31370) %>%
  st_centroid(of_largest_polygon = TRUE) %>%
  st_transform(crs = 4326)
results$geometry[idx] <- converted_to_points$geometry

## Popp, gebouwen omzetten in punten, straten in lijnen
table(results$shape_type)
idx  <- which(results$geometry_type %in% c("POLYGON", "MULTIPOLYGON") & results$datasource %in% "geo.rds" & results$spatial_map %in% "popp")
test <- results[idx, ]
plot(test[, "shape_type", drop = FALSE], key.width = lcm(7))
idx  <- which(results$geometry_type %in% c("POLYGON", "MULTIPOLYGON") &
                results$datasource %in% "geo.rds" & results$spatial_map %in% "popp" &
                results$shape_type %in% c("bebouwd", "Brug", "publiek", "religieus"))
test <- results[idx, ]
plot(test[, "shape_type", drop = FALSE], key.width = lcm(3))
converted_to_points <- st_transform(test, crs = 31370) %>% st_centroid(of_largest_polygon = TRUE) %>% st_transform(crs = 4326)
results$geometry[idx] <- converted_to_points$geometry
idx  <- which(results$geometry_type %in% c("POLYGON", "MULTIPOLYGON") &
                results$datasource %in% "geo.rds" & results$spatial_map %in% "popp" &
                results$shape_type %in% c("straat/openbaar domein"))
test <- results[idx, ]
converted_to_lines <- st_transform(test, crs = 31370) %>% st_cast("MULTILINESTRING") %>% st_transform(crs = 4326)
results$geometry[idx] <- converted_to_lines$geometry


## Straten Ward
idx  <- which(results$geometry_type %in% c("POLYGON", "MULTIPOLYGON") & results$datasource %in% "geo.rds" & results$spatial_map %in% "straten_17de_eeuw")
test <- results[idx, ]
plot(test[, "shape_type", drop = FALSE], key.width = lcm(7))
idx  <- which(results$geometry_type %in% c("POLYGON", "MULTIPOLYGON") & results$datasource %in% "geo.rds" & results$spatial_map %in% "straten_17de_eeuw" &
                results$shape_type %in% c("gebouwen", "parochies"))
test <- results[idx, ]
converted_to_points <- st_transform(test, crs = 31370) %>% st_centroid(of_largest_polygon = TRUE) %>% st_transform(crs = 4326)
results$geometry[idx] <- converted_to_points$geometry
idx  <- which(results$geometry_type %in% c("POLYGON", "MULTIPOLYGON") & results$datasource %in% "geo.rds" & results$spatial_map %in% "straten_17de_eeuw" &
                results$shape_type %in% c("straten"))
test <- results[idx, ]
converted_to_lines <- st_transform(test, crs = 31370) %>% st_cast("MULTILINESTRING") %>% st_transform(crs = 4326)
results$geometry[idx] <- converted_to_lines$geometry

## Gemeentes: polygon gemeente 1830 naar gemeentecentra 1801
idx  <- which(results$geometry_type %in% c("POLYGON", "MULTIPOLYGON") & results$datasource %in% "geo.rds" &
                results$spatial_map %in% "gemeenten_1830")
test <- results[idx, ]
plot(test[, "chunk_text"])

mapping <- st_join(st_as_sf(maps$gemeenten_1830)      %>% st_transform(crs = 31370),
                   st_as_sf(maps$gemeentecentra_1801) %>% st_transform(crs = 31370),
                   join = st_contains, suffix = c("", ".centrum"))
mapping <- st_transform(mapping, crs = 4326)
mapping <- mapping[, c("shape_id", "shape_id.centrum", "LABEL", "LABEL.centrum")]
mapping$geometry <- NULL
mapping <- merge(mapping, st_as_sf(maps$gemeentecentra_1801)[, c("shape_id", "geometry")],
                 by.x = "shape_id.centrum", by.y = "shape_id", all.x = TRUE)
mapping <- st_sf(mapping, sf_column_name = "geometry")
mapping <- subset(mapping, !is.na(shape_id) & !is.na(shape_id.centrum))
mapping$distance <- stringdist(mapping$LABEL, mapping$LABEL.centrum, method = "osa", weight = c(d = 1, i = 0.1, s = 1, t = 1))
mapping <- mapping[order(mapping$distance, decreasing = FALSE), ]
#View(subset(mapping, shape_id %in% mapping$shape_id[which(duplicated(mapping$shape_id))]))
mapping <- mapping[!duplicated(mapping$shape_id), ]
mapping <- subset(mapping, shape_id %in% test$shape_id)
rownames(mapping) <- mapping$shape_id
mapping <- mapping[test$shape_id, ]
test$geometry <- mapping$geometry
results$geometry[idx] <- test$geometry
table(st_geometry_type(test$geometry))

## SAVE TO DB
results$geometry_type <- as.character(st_geometry_type(results$geometry))
table(results$geometry_type, results$datasource)
table(results$geometry_type, results$datasource)
table(results$spatial_map, results$geometry_type, results$datasource)

test <- subset(results, spatial_map %in% "popp")
test <- subset(results, spatial_map %in% "gemeenten_1830" & geometry_type %in% "POLYGON")

con <- DBconnection()
tosave <- subset(results, datasource %in% c("geo.rds", "geo_stedenatlas.rds"))
dbWriteTable(con, name = c("getuigenissen", "getuigenissen_brugge_geocoded_simplified"),
             value = tosave,
             append = FALSE)
dbDisconnect(con)

table(as.character(st_geometry_type(results$geometry)), results$geometry_type, exclude = c())
table(as.character(st_geometry_type(results$geometry)), exclude = c())
setwd("/home/jwijffels/getuigenissen/shp")
saveRDS(tosave, "getuigenissen_brugge_geocoded_simplified.rds")

con <- DBconnection()
test <- st_read(con, layer = c("getuigenissen", "getuigenissen_brugge_geocoded"), as_tibble = FALSE)
dbDisconnect(con)

con <- DBconnection(user = "qgis")
test <- st_read(con, layer = c("getuigenissen", "getuigenissen_brugge_geocoded_simplified"), as_tibble = FALSE)
dbDisconnect(con)

#st_write(results, "PG:dbname=postgis", "sids", layer_options = "OVERWRITE=true")
