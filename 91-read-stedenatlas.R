
clean_spatial_annotations <- function(x){
  x$type <- ifelse(x$comment %in% "onmogelijk te geocoderen", NA, x$type)
  fields <- c("annotation_time", "chunk_id", "chunk_text", "doc_id", "text",
              "type", "spatial_map", "spatial_label", "spatial_type", "shape_id", "comment", "geometry")
  #x <- x[, fields]
  #x <- x[order(as.integer(x$chunk_id), decreasing = FALSE), ]
  x <- x[order(x$annotation_time, decreasing = FALSE), ]
  rownames(x) <- NULL

  for(i in seq_len(nrow(x))){
    if(st_is_empty(x$geometry[i])){
      mapname <- x$spatial_map[i]
      shape_identifier <- x$shape_id[i]
      if(!is.na(mapname)){
        geom <- st_as_sf(subset(dashdata$maps[[mapname]], shape_id == shape_identifier))$geometry
        x$geometry[i] <- geom
      }
    }
  }
  x$spatial_type <- st_geometry_type(x$geometry)
  x <- x[, fields]
  x
}
x         <- read_annotations(with_filters = F)
x         <- subset(x, modus %in% "prod" & datasource %in% "geo_stedenatlas.rds")
locations <- clean_spatial_annotations(x)
locations <- locations[order(as.integer(locations$chunk_id), decreasing = FALSE), ]
rownames(locations) <- NULL

sf::write_sf(subset(locations, spatial_type %in% "POLYGON"), dsn = "geo_stedenatlas.shp")
sf::write_sf(subset(locations, spatial_type %in% "POINT"), dsn = "geo_stedenatlas.shp")

#locations <- subset(locations, locations$comment %in% "OK")
m <- leaflet(data = dashdata$maps$brugge)
maps <- basisKaart()
m <- maps$map
locations$LABEL <- ifelse(is.na(locations$spatial_label), locations$chunk_text,
                          paste(locations$chunk_text, locations$spatial_label, sep = " ==> "))
areas <- subset(locations, spatial_type %in% "POLYGON")
if(nrow(areas) > 0){
  m <- addPolygons(m, data = areas, weight = 1, fill = TRUE, fillOpacity = 0, label = ~LABEL)
}
areas <- subset(locations, spatial_type %in% "POINT")
if(nrow(areas) > 0){
  m <- addCircles(m, data = areas, label = ~LABEL, radius = 3, col = "blue")
}
areas <- subset(locations, spatial_type %in% "LINESTRING")
if(nrow(areas) > 0){
  m <- addPolylines(m, data = areas, color = "red", label = ~LABEL,
                    weight = 1, fill = TRUE, fillOpacity = 0.3)
}
m

sf::as_Spatial(locations)

geom <- st_as_sf(subset(dashdata$maps[["popp"]], shape_id == "popp::popp::GRG_15901"))
x$geometry[i] <- geom

m <- leaflet(data = dashdata$kaart)
m

leaflet(subset(locations, spatial_type %in% "POLYGON")) %>%
  addProviderTiles("CartoDB.Positron") %>%
  addPolygons(color = "green")

table(x$type)
table(x$spatial_map)
mapply(map = x$spatial_map, map_id = x$spatial_id, geom = x$geometry)
geom               <- st_as_sf(locatie$map)

dashdata$maps$popp$shape_id ==
