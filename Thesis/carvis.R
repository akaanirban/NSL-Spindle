require(ggplot2);
source("database.R");
# Sparse Region: $(40000, 106800)$ to $(53000, 114000)$.
# Dense Region $(50000, 100000)$ to $(51000, 102000)$
main <- function() {
    dbconn <- getdb();
    query <- { "select x,y, speed from node_positions where timestamp=21903" }
    df_results <- dbGetQuery(dbconn, query)
    print(df_results)
    p <- ggplot(df_results, aes(x, y)) + 
        geom_point(aes(colour=speed), size=0.5) + 
        geom_rect(aes(xmin=40000, xmax=53000, ymin=106800, ymax=114000), fill='darkorchid4', alpha=.01) +
        geom_rect(aes(xmin=50000, xmax=51000, ymin=100000, ymax=102000), fill='orange3', alpha=.01);
    print(p)
    ggsave(file="binImages/cars.png")
}

main();
