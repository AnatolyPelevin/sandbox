package com.ringcentral.analytics.adobe.service;


import com.ringcentral.analytics.adobe.AdobeException;
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
import java.time.LocalDate;
import java.util.UUID;
//import java.nio.charset.StandardCharsets;

public class HDFSService {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSService.class);
    private final FileSystem fileSystem;

    public HDFSService(Configuration conf) {
        try {
            fileSystem = FileSystem.getLocal(conf);
          //TODO return
            //  fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            throw new AdobeException("Unable to instantiate hdfs.", e);
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

//    public void append(Path path, String row) throws IOException {
////      //  System.setProperty("hadoop.home.dir", "C:/Users/anatoly.pelevin/Documents/Hadoop/");
// //     try (FSDataOutputStream os = fileSystem.exists(path) ? fileSystem.append(path) : fileSystem.create(path);
////
////        try (FSDataOutputStream os  = fileSystem.create(new Path(path.toString(), LocalDate.now().toString() + UUID.randomUUID().toString()));
////             PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)))) {
////            writer.print(row);
////            writer.flush();
////            os.flush();
////        }
//    }

    public InputStream getFileAsStream(Path path) throws IOException {
        return fileSystem.open(path);
    }
}
