package com.ringcentral.analytics.anaplan.service;


import com.ringcentral.analytics.anaplan.AnaplanException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public class HDFSService {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSService.class);
    private final FileSystem fileSystem;

    public HDFSService(Configuration conf) {
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            throw new AnaplanException("Unable to instantiate hdfs.", e);
        }
    }

    FileSystem getFs() {
        return fileSystem;
    }

    public boolean delete(Path path) throws IOException {
        return delete(path, false);
    }

    public boolean delete(Path path, boolean recursive) throws IOException {
        if (fileSystem.exists(path)) {
            return fileSystem.delete(path, recursive);
        }
        return false;
    }

    public void makeDirs(Path path) throws IOException {
        fileSystem.mkdirs(path);
    }

    public void rename(Path src, Path dst) throws IOException {
        fileSystem.rename(src, dst);
    }

    public void append(Path path, String row) throws IOException {
        try (FSDataOutputStream os = fileSystem.exists(path) ? fileSystem.append(path) : fileSystem.create(path);
             PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)))) {
            writer.print(row);
            writer.flush();
            os.flush();
        }
    }

    public InputStream getFileAsStream(Path path) throws IOException {
        return fileSystem.open(path);
    }
}
