package com.ringcentral.analytics.cloudera.service;

import com.ringcentral.analytics.cloudera.ClouderaException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class HDFSService {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSService.class);
    private final FileSystem fileSystem;

    public HDFSService(Configuration conf) {
        try {
            //fileSystem = FileSystem.getLocal(conf);
            //TODO return
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            throw new ClouderaException("Unable to instantiate hdfs.", e);
        }
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public boolean delete(Path path) throws IOException {
        return delete(path, false);
    }

    public boolean delete(Path path, boolean recursive) throws IOException {
        return fileSystem.exists(path) && fileSystem.delete(path, recursive);
    }

    public void makeDirs(Path path) throws IOException {
        fileSystem.mkdirs(path);
    }

    public void rename(Path src, Path dst) throws IOException {
        fileSystem.rename(src, dst);
    }


    public InputStream getFileAsStream(Path path) throws IOException {
        return fileSystem.open(path);
    }
}
