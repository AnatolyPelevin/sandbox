package com.ringcentral.analytics.adobe.service;

import com.ringcentral.analytics.adobe.utils.ImportOptions;
import com.ringcentral.analytics.adobe.utils.reports.ReportLoader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import com.ringcentral.analytics.adobe.utils.writer.RecordWriter;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionContext {
    private static final String SLASH = "/";

    private final ImportOptions options;
    private final HttpService http;
    private final HDFSService hdfs;
    private final ReportLoader reportLoader;
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);

    public ExecutionContext(ImportOptions options, HttpService http, HDFSService hdfs, ReportLoader reportLoader) {
        this.options = options;
        this.http = http;
        this.hdfs = hdfs;
        this.reportLoader = reportLoader;
    }

    public void start() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException {
        String reportName = reportLoader.getAdobeReportType().name();
        LOG.info("Start processing report: " + reportName);
        Path csvTmpPath = new Path(options.getTmpLocation() + SLASH + reportName + ".csv");
        hdfs.delete(csvTmpPath, true);
        var reportAsListOfStrings = reportLoader.load();
        if (Optional.ofNullable(reportAsListOfStrings).isPresent()) {
            RecordWriter recordWriter = new RecordWriter(hdfs.getFileSystem(), csvTmpPath.toString());
            reportAsListOfStrings.forEach(row -> recordWriter.write(row));
            recordWriter.close();
        }
    }

    public ImportOptions getOptions() {
        return options;
    }

    public HttpService getHttp() {
        return http;
    }

    public HDFSService getHdfs() {
        return hdfs;
    }

    public ReportLoader getReportLoader() {
        return reportLoader;
    }


}
