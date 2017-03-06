require(ggplot2);
source("database.R");

# TODO: separate plot for each m/r job
# TODO (meeting): need this to look readable on black/white
# TODO (meeting): white background black bars

replaceText <- function(row_text) {
    return(gsub("self_clusters\nsparse_positions", "sparse-unclustered",
                gsub("self_clusters\ndense_positions", "dense-unclustered",
                    gsub("clusterinfo_sparse_v1\nsparse_positions", "sparse-clustered",
                        gsub("clusterinfo_dense_v1\ndense_positions", "dense-clustered", row_text)))))
}

getRuns <- function(dbconn, mrJob) {
    query <- {paste("select * from normed_runs where clustertable !='self_clusters' and mapreducename='", paste(mrJob, "'", sep=""), sep="")}
    print(query);
    df_results <- transform(dbGetQuery(dbconn, query), configuration=replaceText(interaction(clustertable, filtertablename, paste(sapply(windowsizems,function(x) x / 1000), "s"), sep="\n")))
    normed_pot <- ggplot(df_results, aes(x=configuration, y=normed_total_bytes)) + 
        theme_bw() +
        geom_bar(aes(fill = filtertablename), stat="identity") + 
        scale_y_continuous(name="Normalized Bytes to Middleware", limits=c(0,1)) +
        theme(text=element_text(size=15), axis.text.x=element_text(angle=60, vjust=0.5),plot.title=element_text(hjust = 0.5)) + 
        labs(title = paste(mrJob, "Normalized Bytes Transferred to Cloud")) +
        scale_fill_grey()
    normed_plot_path <- paste("binImages/", paste(mrJob, "runplot-normalized.pdf", sep="-"), sep="");
    ggsave(file=normed_plot_path)
    query <- {paste("select * from normed_runs where mapreducename='", paste(mrJob, "'", sep=""), sep="")}
    df_results <- transform(dbGetQuery(dbconn, query), configuration=replaceText(interaction(clustertable, filtertablename, paste(sapply(windowsizems,function(x) x / 1000), "s"), sep="\n")))
    total_plot <- ggplot(df_results, aes(x=configuration, y=avg_total_bytes)) + 
        theme_bw() +
        geom_bar(aes(fill = filtertablename), stat="identity") + 
        theme(text=element_text(size=15), axis.text.x = element_text(angle=60, vjust=0.5), plot.title=element_text(hjust = 0.5)) + 
        labs(title = paste(mrJob, "Total Bytes Transferred to Cloud")) + 
        scale_fill_grey()
    avg_plot_path <- paste("binImages/", paste(mrJob, "runplot.pdf", sep="-"), sep="");
    ggsave(file=avg_plot_path)
}

compareAll <- function(dbconn) {
    query <- {
        "select * from normed_runs";
    }
    df_results <- dbGetQuery(dbconn, query);
    print(df_results);
}

main <- function() {
    dbconn <- getdb();
    getRuns(dbconn, "speedSum");
    getRuns(dbconn, "geoMapped");
    getRuns(dbconn, "geoFiltered");
    compareAll(dbconn);
}

main();

