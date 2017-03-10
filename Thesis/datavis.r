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

rescaleWindow <- function(windowsizems) {
    return(paste(sapply(windowsizems,function(x) x / 1000), "s", sep=""));
}

getRuns <- function(dbconn, mrJob) {
    xformMr <- function(df_results) {
        return(transform(df_results, configuration=replaceText(interaction(clustertable, filtertablename, rescaleWindow(windowsizems), sep="\n"))));
    }
    formatPlot <- function(plot) {
       return(plot + 
        theme_bw() +
        geom_bar(aes(fill = filtertablename), stat="identity") + 
        labs(x="Simulator Configuration", fill="Region Density") +
        theme(text=element_text(size=15), axis.text.x=element_text(angle=60, vjust=0.5),plot.title=element_text(hjust = 0.5)) + 
        scale_fill_grey());
    }
    query <- {paste("select * from normed_runs where clustertable !='self_clusters' and mapreducename='", paste(mrJob, "'", sep=""), sep="")}
    df_results <- xformMr(dbGetQuery(dbconn, query))
    normed_pot <- formatPlot(ggplot(df_results, aes(x=configuration, y=normed_total_bytes)) + 
        scale_y_continuous(name="Normalized Bytes to Middleware", limits=c(0,1)) +
        labs(title = paste(mrJob, "Normalized Bytes Transferred to Cloud")));
    normed_plot_path <- paste("binImages/", paste(mrJob, "runplot-normalized.pdf", sep="-"), sep="");
    ggsave(file=normed_plot_path)
    query <- {paste("select * from normed_runs where mapreducename='", paste(mrJob, "'", sep=""), sep="")}
    df_results <- xformMr(dbGetQuery(dbconn, query))
    total_plot <- formatPlot(ggplot(df_results, aes(x=configuration, y=avg_total_bytes)) + 
        geom_bar(aes(fill = filtertablename), stat="identity") + 
        labs(title = paste(mrJob, "Total Bytes Transferred to Cloud")));
    avg_plot_path <- paste("binImages/", paste(mrJob, "runplot.pdf", sep="-"), sep="");
    ggsave(file=avg_plot_path)
}

renameClusterTable <- function(in_text) {
    return (gsub("self_clusters", "unclustered", gsub("clusterinfo_", "", gsub("_v1", " clusters", in_text))));
}


mkGraphFilterType <- function(df_results) {
    xformed_results <- transform(df_results, configuration=gsub("\\.", "\n", renameClusterTable(interaction(clustertable, rescaleWindow(windowsizems),mapreducename))))
    plot <- ggplot(xformed_results, aes(x=configuration,y=avg_total_bytes)) +
        theme_bw() +
        geom_bar(aes(fill=mapreducename), stat="identity") +
        scale_fill_grey() + 
        theme(axis.text.x = element_text(angle=90, vjust=1), text=element_text(size=15)) +
        scale_x_discrete(expand = c(0,.01)) +
        labs(x="Simulator Configuration", y="Average Total Bytes", fill="MR Job")
    return(plot);
}

compareSizes <- function(dbconn) {
    query_header <- {
        "select configid, clustertable, windowsizems, mapreducename, avg_total_bytes from normed_runs";
    }
    dense_query = paste(query_header, "where filtertablename='dense_positions'", sep=" ");
    dense_results <- dbGetQuery(dbconn, dense_query);
    dense_plot <- mkGraphFilterType(dense_results) + labs(title="Dense Region Total Bytes Transferred to Cloud");
    ggsave(file="binImages/denseConfigs.pdf", width=13);

    sparse_query = paste(query_header, "where filtertablename='sparse_positions'", sep=" ");
    sparse_results <- dbGetQuery(dbconn, sparse_query);
    sparse_plot <- mkGraphFilterType(sparse_results) + labs(title="Sparse Region Total Bytes Transferred to Cloud");
    ggsave(file="binImages/sparseConfigs.pdf", width=13);
}

main <- function() {
    dbconn <- getdb();
    getRuns(dbconn, "speedSum");
    getRuns(dbconn, "geoMapped");
    getRuns(dbconn, "geoFiltered");
    compareSizes(dbconn);
}

main();

