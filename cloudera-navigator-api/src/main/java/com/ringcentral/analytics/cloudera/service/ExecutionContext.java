package com.ringcentral.analytics.cloudera.service;

import com.ringcentral.analytics.cloudera.ClouderaException;
import com.ringcentral.analytics.cloudera.utils.ImportOptions;
import com.ringcentral.analytics.cloudera.utils.reader.RecordReader;
import com.ringcentral.analytics.cloudera.utils.response.TableResponse;
import com.ringcentral.analytics.cloudera.utils.writer.RecordWriter;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

public class ExecutionContext {
    private static final String SLASH = "/";

    private final ImportOptions options;
    private final HttpService http;
    private final HDFSService hdfs;

    public ExecutionContext(ImportOptions options, HttpService http, HDFSService hdfs) {
        this.options = options;
        this.http = http;
        this.hdfs = hdfs;
    }

    public void start() throws URISyntaxException, IOException {
        boolean work = true;
        int offset = options.getOffset();
        StringBuilder stringBuilder = new StringBuilder(options.getTmpLocation());
        stringBuilder.append(SLASH).append(options.getDatabaseName()).append(".csv");
        Path csvTmpPath = new Path(stringBuilder.toString());
        hdfs.delete(csvTmpPath, true);

        do {
            List<String> reportAsListOfStrings = http.getTableUsage(offset, null,null,null)
                    .stream()
                    .map(TableResponse::toString)
                    .collect(Collectors.toList());

            if (Optional.ofNullable(reportAsListOfStrings).isPresent()) {
                RecordWriter recordWriter = new RecordWriter(hdfs.getFileSystem(), csvTmpPath.toString(), options.getDatabaseName() + "_" + offset);
                reportAsListOfStrings.forEach(row -> recordWriter.write(row));
                recordWriter.close();
            }
            if (Optional.ofNullable(reportAsListOfStrings).isPresent() && reportAsListOfStrings.size() >= 100) {
                offset = offset + 100;
            } else {
                work = false;
            }
        } while(work);
    }

    public void startPerTable() throws IOException {
        StringBuilder stringBuilder = new StringBuilder(options.getTmpLocation());
        stringBuilder.append(SLASH).append(options.getDatabaseName()).append("_per_table").append(".csv");
        Path csvTmpPath = new Path(stringBuilder.toString());
        hdfs.delete(csvTmpPath, true);


        getTableList().forEach(tableName-> {
            try {
                perTable(csvTmpPath, tableName);
            } catch (URISyntaxException | IOException e) {
                throw new ClouderaException("Cant load data: ", e);
            }
        });
    }

    private Set<String> getTableList() throws IOException {
        Path csvTmpPath = new Path(options.getTableListLocation());
        RecordReader recordReader = null;
        Set<String> tables = new HashSet<>();
        try {
             recordReader = new RecordReader(hdfs.getFileSystem(), csvTmpPath.toString(), options.getDatabaseName());

            try {
                String thisLine = null;
                while ((thisLine = recordReader.nextRecord()) != null) {
                    tables.addAll( Arrays.stream(thisLine.split("\n")).map(String::trim).collect(Collectors.toSet()));
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        } finally {
            recordReader.close();
        }
        return tables;
    }

    private void perTable(Path csvTmpPath, String tableName) throws URISyntaxException, IOException {
        int offset = 0;
        boolean work;
        do {
            List<TableResponse> reports = http.getTableUsage(offset, null,null, tableName);
            work = false;
            if (Optional.ofNullable(reports).isPresent() && reports.size() > 0) {
//                work =  reports.stream()
//                        .filter(tableResponse -> !(tableResponse.getUsername().contains("dtx") && "DESCRIBE_TABLE".equals(tableResponse.getCommand())))
//                        .findFirst()
//                        .isPresent() ? false : true;

                work =  reports.stream()
                        .filter(tableResponse -> ("DML".equals(tableResponse.getCommand()) || "QUERY".equals(tableResponse.getCommand())))
                        .findFirst()
                        .isPresent() ? false : true;

                List<String> reportAsListOfStrings = reports.stream()
                        .map(TableResponse::toString)
                        .collect(Collectors.toList());

                RecordWriter recordWriter = new RecordWriter(hdfs.getFileSystem(), csvTmpPath.toString(), tableName + "_" + offset);
                reportAsListOfStrings.forEach(row -> recordWriter.write(row));
                recordWriter.close();
            }
            offset = work ? offset + 100 : offset;

        } while(work);
    }

}
