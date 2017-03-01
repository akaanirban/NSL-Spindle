require("RPostgreSQL");

getdb <- function() {
  dbpass <- { "spindle" }
  dbhost <- { "postgres.spindl.network" }
  dbdriver <- dbDriver("PostgreSQL");
  dbconn <- dbConnect(dbdriver, dbname="postgres", host=dbhost, port=5432, user="postgres", password=dbpass)
  return(dbconn);
}