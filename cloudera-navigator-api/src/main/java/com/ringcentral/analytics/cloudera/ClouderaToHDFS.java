package com.ringcentral.analytics.cloudera;

import com.ringcentral.analytics.cloudera.service.ExecutionContext;
import com.ringcentral.analytics.cloudera.service.HDFSService;
import com.ringcentral.analytics.cloudera.service.HttpService;
import com.ringcentral.analytics.cloudera.utils.ImportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ClouderaToHDFS {
   private static final Logger LOG = LoggerFactory.getLogger(ClouderaToHDFS.class);
    private final ImportOptions options;

    public ClouderaToHDFS(ImportOptions options) {
        this.options = options;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("file.encoding", "UTF-8");
        LOG.info("Parse Cloudera application options");
        //TODO remove empty arg
        if (!Optional.ofNullable(args).isPresent()) {
            args = new String[] {"element1","element2","element3"};
            //args = new String[] {"--tmp-location=D:\\WORK\\ADOBE_API\\Adobe_auth", "--adobe-key-path=D:\\WORK\\ADOBE_API\\Adobe_auth"};
        }
        ImportOptions options = new ImportOptions(args);

        LOG.info("Start Cloudera processing");
        new ClouderaToHDFS(options).start();
    }

    private void start() throws Exception {
        ServiceFactory serviceFactory = new ServiceFactory();
        HttpService http = serviceFactory.createHttpService(options);
        HDFSService hdfs = serviceFactory.createHDFS();
        ExecutionContext context = new ExecutionContext(options, http, hdfs);
        //context.start();
        context.startPerTable();
    }
}
