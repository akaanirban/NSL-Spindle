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

replaceText <- function(row_text) {
    return(gsub("clusterinfo_sparse_v1\nsparse_positions", "clustered-sparse",
                gsub("clusterinfo_dense_v1\ndense_positions", "clustered-dense", row_text)))
}

getRuns <- function(dbconn, mrJob) {
    query <- {paste("select * from normed_runs where clustertable !='self_clusters' and mapreducename='", paste(mrJob, "'", sep=""), sep="")}
    print(query);
    df_results <- transform(dbGetQuery(dbconn, query), configuration=replaceText(interaction(clustertable, filtertablename, paste(sapply(windowsizems,function(x) x / 1000), "s"), sep="\n")))
    print(df_results);
    normed_pot <- ggplot(df_results, aes(x=configuration, y=normed_total_bytes)) + 
        geom_bar(aes(fill = filtertablename), stat="identity") + 
        scale_y_continuous(name="Normalized Bytes to Middleware", limits=c(0,1)) +
        theme(axis.text.x=element_text(angle=60, vjust=0.5),plot.title=element_text(hjust = 0.5)) + 
        labs(title = "speedSum Normalized Bytes Transferred to Cloud");
    normed_plot_path <- paste("binImages/", paste(mrJob, "runplot-normalized.pdf", sep="-"), sep="");
    ggsave(file=normed_plot_path)
    total_plot <- ggplot(df_results, aes(x=configuration, y=avg_total_bytes)) + 
        geom_bar(aes(fill = filtertablename), stat="identity") + 
        theme(axis.text.x = element_text(angle=90), plot.title=element_text(hjust = 0.5)) + 
        labs(title = "speedSum Total Bytes Transferred to Cloud");
    avg_plot_path <- paste("binImages/", paste(mrJob, "runplot.pdf", sep="-"), sep="");
    ggsave(file=avg_plot_path)
}

main <- function() {
    dbconn <- getdb();
    getRuns(dbconn, "speedSum");
    getRuns(dbconn, "geoMapped");
}

main();
