package com.ringcentral.analytics.cloudera.utils.reader;

import com.ringcentral.analytics.cloudera.ClouderaException;
import com.ringcentral.analytics.cloudera.service.HDFSService;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class RecordReader {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSService.class);
    private FSDataInputStream fsDataInputStream;
    private BufferedReader bufferedReader;

    public RecordReader(FileSystem fileSystem, String path, String reportName) throws IOException {
        fsDataInputStream = fileSystem.open(new Path(path, reportName + ".txt"));
        bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
    }

    public String nextRecord() {
        try {
            return bufferedReader.readLine();
        } catch (IOException e) {
            throw new ClouderaException("Cant read file", e);
        }
    }

    public void close() {
        LOG.info("Closing reader");
        IOUtils.closeStream(bufferedReader);
        IOUtils.closeStream(fsDataInputStream);
    }
}
