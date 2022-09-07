library(sp)
library(rgeos)
library(sf)
library(magrittr)
DBconnection <- function(){
  con <- dbConnect(drv = PostgreSQL(),
                   user = Sys.getenv("GETUIGENISSEN_GEOCODING_USER"),
                   dbname = Sys.getenv("GETUIGENISSEN_GEOCODING_DB"),
                   password = Sys.getenv("GETUIGENISSEN_GEOCODING_PWD"),
                   host = Sys.getenv("GETUIGENISSEN_GEOCODING_HOST"),
                   port = Sys.getenv("GETUIGENISSEN_GEOCODING_PORT"))
  con
}
read_annotations <- function(chunk_text = NULL, with_filters = TRUE){
  con <- DBconnection()
  on.exit(dbDisconnect(con))
  if(is.null(chunk_text) || length(chunk_text) == 0){
    x <- st_read(con, layer = c("getuigenissen", "locations_geocoded"), as_tibble = FALSE)
  }else{
    sql <- glue_sql("SELECT * FROM getuigenissen.locations_geocoded WHERE chunk_text IN ({locations*})",
                    locations = chunk_text, .con = con)
    x <- st_read(con, query = sql)
  }
  if(nrow(x) > 0 && with_filters){
    x <- subset(x, comment %in% "gazetteer" | datasource %in% dashdata$filenaam)
    x <- subset(x, modus %in% "prod")
    #c("LINESTRING", "POINT", "POLYGON")
  }
  x
}
read_manual_annotations <- function(){
  con <- DBconnection()
  on.exit(dbDisconnect(con))
  x <- st_read(con,
               query = "SELECT modus, datasource, shape_id, chunk_text as label, type, geometry, comment FROM getuigenissen.locations_geocoded WHERE type = 'manual'")
  x$LABEL <- x$label
  if(nrow(x) > 0){
    x <- subset(x, modus %in% "prod")
  }
  x
}
update_maps_manual <- function(){
  areas                <- read_manual_annotations()
  if(nrow(areas) > 0){
    areas$geom_type <- as.character(st_geometry_type(areas))
    areas           <- split(areas[c("shape_id", "LABEL", "type", "geometry")], areas$geom_type)
    if("POINT" %in% names(areas)){
      dashdata$maps$manual_points <<- as_Spatial(areas$POINT)
    }else{
      dashdata$maps$manual_points <<- NULL
    }
    if("POLYGON" %in% names(areas)){
      dashdata$maps$manual_polygons <<- as_Spatial(areas$POLYGON)
    }else{
      dashdata$maps$manual_polygons <<- NULL
    }
    if("LINESTRING" %in% names(areas)){
      dashdata$maps$manual_lines <<- as_Spatial(areas$LINESTRING)
    }else{
      dashdata$maps$manual_lines <<- NULL
    }
  }
  invisible()
}
## Geographical data
data("BE_ADMIN_PROVINCE", package = "BelgiumMaps.StatBel")
data("BE_ADMIN_DISTRICT", package = "BelgiumMaps.StatBel")
data("BE_ADMIN_MUNTY",    package = "BelgiumMaps.StatBel")
data("BE_ADMIN_SECTORS",  package = "BelgiumMaps.StatBel")
BE_ADMIN_PROVINCE$shape_id  <- BE_ADMIN_PROVINCE$CD_PROV_REFNIS
BE_ADMIN_PROVINCE$LABEL     <- sprintf("%s - %s", BE_ADMIN_PROVINCE$TX_PROV_DESCR_NL, BE_ADMIN_PROVINCE$TX_PROV_DESCR_FR)
BE_ADMIN_DISTRICT$shape_id  <- BE_ADMIN_DISTRICT$CD_DSTR_REFNIS
BE_ADMIN_MUNTY$shape_id     <- BE_ADMIN_MUNTY$CD_MUNTY_REFNIS
BE_ADMIN_MUNTY$LABEL        <- sprintf("%s - %s", BE_ADMIN_MUNTY$TX_MUNTY_DESCR_NL,
                                       BE_ADMIN_MUNTY$TX_MUNTY_DESCR_FR)
BE_ADMIN_SECTORS$shape_id   <- BE_ADMIN_SECTORS$objectid
BE_ADMIN_SECTORS$LABEL      <- sprintf("%s - %s", BE_ADMIN_SECTORS$TX_SECTOR_DESCR_NL,
                                       BE_ADMIN_SECTORS$TX_SECTOR_DESCR_FR)
vlaanderen            <- BE_ADMIN_DISTRICT
vlaanderen            <- subset(vlaanderen, TX_RGN_DESCR_NL %in% "Vlaams Gewest")
vlaanderen$LABEL      <- vlaanderen$TX_ADM_DSTR_DESCR_NL
westvlaanderen        <- subset(BE_ADMIN_SECTORS, TX_PROV_DESCR_NL %in% "Provincie West-Vlaanderen")
westvlaanderen$LABEL  <- westvlaanderen$TX_SECTOR_DESCR_NL
brugge_omgeving       <- subset(BE_ADMIN_SECTORS, TX_ADM_DSTR_DESCR_NL %in% "Arrondissement Brugge")
brugge_omgeving$LABEL <- brugge_omgeving$TX_MUNTY_DESCR_NL
brugge                <- subset(BE_ADMIN_SECTORS, TX_MUNTY_DESCR_NL %in% "Brugge")
brugge$LABEL          <- brugge$TX_SECTOR_DESCR_NL
bruggecentrum         <- subset(BE_ADMIN_SECTORS, TX_MUNTY_DESCR_NL %in% "Brugge")
bruggecentrum$LABEL   <- bruggecentrum$TX_SECTOR_DESCR_NL
basisachtergrond      <- subset(BE_ADMIN_SECTORS, TX_MUNTY_DESCR_NL %in% "Brugge")
bbbox <- structure(c(xmin = 3.205804, ymin = 51.194524, xmax = 3.249331, ymax = 51.225888),
                   class = "bbox", crs = structure(list(input = "EPSG:4326",
                                                        wkt = "GEOGCRS[\"WGS 84\",\n    DATUM[\"World Geodetic System 1984\",\n        ELLIPSOID[\"WGS 84\",6378137,298.257223563,\n            LENGTHUNIT[\"metre\",1]]],\n    PRIMEM[\"Greenwich\",0,\n        ANGLEUNIT[\"degree\",0.0174532925199433]],\n    CS[ellipsoidal,2],\n        AXIS[\"geodetic latitude (Lat)\",north,\n            ORDER[1],\n            ANGLEUNIT[\"degree\",0.0174532925199433]],\n        AXIS[\"geodetic longitude (Lon)\",east,\n            ORDER[2],\n            ANGLEUNIT[\"degree\",0.0174532925199433]],\n    USAGE[\n        SCOPE[\"unknown\"],\n        AREA[\"World\"],\n        BBOX[-90,-180,90,180]],\n    ID[\"EPSG\",4326]]"), class = "crs"))
bbbox <- st_bbox(bbbox)
bbbox <- matrix(c(xmin = 3.205804, ymin = 51.194524, xmax = 3.249331, ymax = 51.225888),
                nrow = 2, dimnames = list(c("x", "y"), c("min", "max")))
bbbox <- bbox2SP(n = 51.225888, s = 51.194524, w = 3.205804, e = 3.249331, proj4string = bruggecentrum@proj4string)
bbbox <- st_as_sf(bbbox)
#bruggecentrum <- gIntersection(bruggecentrum, bbbox, byid = T)
bruggecentrum <- st_as_sf(bruggecentrum)
bruggecentrum <- st_intersection(bruggecentrum, bbbox)
bruggecentrum <- as_Spatial(bruggecentrum)

basisachtergrond_bruggecentrum <- st_as_sf(basisachtergrond)
basisachtergrond_bruggecentrum <- st_intersection(basisachtergrond_bruggecentrum, bbbox)
basisachtergrond_bruggecentrum <- as_Spatial(basisachtergrond_bruggecentrum)

dashdata <- list()
dashdata$root_maps <- "/home/jwijffels/getuigenissen"

dashdata$kaart <- vlaanderen
dashdata$filenaam <- "default"
dashdata$wms_link <- "https://geoservices.informatievlaanderen.be/raadpleegdiensten/histcart/wms"
dashdata$file <- "default"
dashdata$save_as <- tempfile(pattern = sprintf("tagged_%s", format(Sys.time(), "%Y%m%d_%H%M%S")), fileext = ".rds")
dashdata$maps <- list()
dashdata$maps$statistical_provincies <- BE_ADMIN_PROVINCE
dashdata$maps$statistical_sectors <- BE_ADMIN_SECTORS
dashdata$maps$statistical_gemeentes <- BE_ADMIN_MUNTY
dashdata$maps$vlaanderen <- vlaanderen
dashdata$maps$westvlaanderen <- westvlaanderen
dashdata$maps$brugge_omgeving <- brugge_omgeving
dashdata$maps$brugge <- brugge
dashdata$maps$bruggecentrum <- bruggecentrum
dashdata$maps$basisachtergrond <- basisachtergrond
dashdata$maps$basisachtergrond_bruggecentrum <- basisachtergrond_bruggecentrum
dashdata$maps$erfgoed_magis <- readRDS(file.path(dashdata$root_maps, "erfgoed_magis.rds"))
dashdata$maps$gemeenten_1830 <- readRDS(file.path(dashdata$root_maps, "gemeenten_1830.rds"))
dashdata$maps$gemeentecentra_1801 <- readRDS(file.path(dashdata$root_maps, "gemeentecentra_1801.rds"))
dashdata$maps$straten_17de_eeuw <- readRDS(file.path(dashdata$root_maps, "straten_17de_eeuw.rds"))
dashdata$maps$popp <- readRDS(file.path(dashdata$root_maps, "popp.rds"))
dashdata$maps$straten_17de_eeuw <- subset(dashdata$maps$straten_17de_eeuw,
                                          shape_type %in% setdiff(c("water", "gebouwen", "straten", "bouwblokken1670", "parochies"), "bouwblokken1670"))
dashdata$maps$straten_17de_eeuw <- dashdata$maps$straten_17de_eeuw[order(dashdata$maps$straten_17de_eeuw$shape_type %in% "parochies", decreasing = TRUE), ]
dashdata$maps$stedenatlas_brugge <- read_annotations(with_filters = FALSE)
dashdata$maps$stedenatlas_brugge <- subset(dashdata$maps$stedenatlas_brugge, datasource %in% "geo_stedenatlas.rds"  & dashdata$maps$stedenatlas_brugge$modus %in% "prod")
dashdata$maps$stedenatlas_brugge$LABEL <- dashdata$maps$stedenatlas_brugge$chunk_text
#dashdata$maps$stedenatlas_brugge <- st_centroid(dashdata$maps$stedenatlas_brugge)
dashdata$maps$stedenatlas_brugge <- st_transform(dashdata$maps$stedenatlas_brugge, crs = 31370) %>%
  st_centroid(of_largest_polygon = TRUE) %>%
  st_transform(crs = 4326)
dashdata$maps$stedenatlas_brugge <- st_as_sf(dashdata$maps$stedenatlas_brugge)
dashdata$maps <- lapply(dashdata$maps, FUN = function(x){
  #x <- x[, intersect(c("shape_id", "LABEL", "shape_type"), names(x))]
  x$LABEL <- ifelse(is.na(x$LABEL), "Unknown", x$LABEL)
  x
})
dashdata$maps <- Map(names(dashdata$maps), dashdata$maps, f=function(type, x){
  x$type <- type
  x
})
dashdata$bbox <- lapply(dashdata$maps[c("bruggecentrum", "brugge", "brugge_omgeving", "westvlaanderen", "vlaanderen")],
                        FUN = function(x) gEnvelope(x))
update_maps_manual()
saveRDS(dashdata$maps, file = file.path(dashdata$root_maps, "dashdata_maps.rds"))
