package com.ringcentral.analytics.adobe.utils.writer;

import com.ringcentral.analytics.adobe.service.HDFSService;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.UUID;

public class RecordWriter {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSService.class);
    private FSDataOutputStream fsDataOutputStream;

    public RecordWriter(FileSystem fileSystem, String path) throws IOException {
        fsDataOutputStream = fileSystem.create(new Path(path, LocalDate.now().toString() + UUID.randomUUID().toString()));
    }

    public void write(String record) {
        try {
            fsDataOutputStream.write((record + "\r\n").getBytes());
            fsDataOutputStream.hflush();
            System.out.println(record);
        } catch (IOException e) {
            LOG.error("Unable to write invalid record to file", e);
        }
    }


    public void close() {
        LOG.info("Closing writer");
        IOUtils.closeStream(fsDataOutputStream);
    }
}

