require(ggplot2);
require("RPostgreSQL");

getdb <- function() {
    dbpass <- { "spindle" }
    dbhost <- { "postgres.spindl.network" }
    dbdriver <- dbDriver("PostgreSQL");
    dbconn <- dbConnect(dbdriver, dbname="postgres", host=dbhost, port=5432, user="postgres", password=dbpass)
    return(dbconn);
}

# TODO: separate plot for each m/r job

getRuns <- function(dbconn, mrJob) {
    query <- {paste("select * from normed_runs where  mapreducename='", paste(mrJob, "'", sep=""), sep="")}
    print(query);
    df_results <- transform(dbGetQuery(dbconn, query), uniqueconfig=interaction(clustertable, filtertablename, windowsizems))
    print(df_results);
    normed_pot <- ggplot(df_results, aes(x=uniqueconfig, y=normed_total_bytes)) + geom_bar(aes(fill = filtertablename), stat="identity") + theme(axis.text.x = element_text(angle=90), plot.title=element_text(hjust = 0.5)) + labs(title = "speedSum Normalized Bytes Transferred to Cloud");
    normed_plot_path <- paste(mrJob, "runplot-normalized.pdf", sep="-");
    ggsave(file=normed_plot_path)
    total_plot <- ggplot(df_results, aes(x=uniqueconfig, y=avg_total_bytes)) + geom_bar(aes(fill = filtertablename), stat="identity") + theme(axis.text.x = element_text(angle=90), plot.title=element_text(hjust = 0.5)) + labs(title = "speedSum Normalized Bytes Transferred to Cloud");
    avg_plot_path <- paste(mrJob, "runplot.pdf", sep="-");
    ggsave(file=avg_plot_path)
}

main <- function() {
    dbconn <- getdb();
    getRuns(dbconn, "speedSum");
}

main();
