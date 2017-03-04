require(ggplot2);
source("database.R");
# Sparse Region: $(40000, 106800)$ to $(53000, 114000)$.
# Dense Region $(50000, 100000)$ to $(51000, 102000)$

# TODO: change speed coloring to something with higher contrast
# TODO: use outlines for boxen
main <- function() {
    dbconn <- getdb();
    query <- { "select x,y, speed from node_positions where timestamp=21903" }
    df_results <- dbGetQuery(dbconn, query)
    print(df_results)
    p <- ggplot(df_results, aes(x, y)) + 
        theme_bw() +
        geom_rect(aes(xmin=47150, xmax=53000, ymin=106801, ymax=112545), color='darkblue', linetype=2, alpha=0) +
        geom_rect(aes(xmin=50000, xmax=51000, ymin=100000, ymax=102000), color='darkred', linetype=1, alpha=0) +
        geom_point(aes(colour=speed), size=0.5) + scale_color_gradient(low='darkred', high='green', trans='log')
    ggsave(file="binImages/cars.png")
}

main();
