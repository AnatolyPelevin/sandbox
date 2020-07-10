package com.ringcentral.analytics.anaplan;

import com.ringcentral.analytics.anaplan.service.HDFSService;
import com.ringcentral.analytics.anaplan.service.HttpService;
import com.ringcentral.analytics.anaplan.service.handlers.ServiceFactory;
import com.ringcentral.analytics.anaplan.utils.ImportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class AnaplanToHDFS {
    private static final Logger LOG = LoggerFactory.getLogger(AnaplanToHDFS.class);
    private final ImportOptions options;

    public AnaplanToHDFS(ImportOptions options) {
        this.options = options;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("file.encoding", "UTF-8");
        LOG.info("Parse Anaplan application options");
        if (!Optional.ofNullable(args).isPresent()) {
            args = new String[] {"element1","element2","element3"};;
        }
        ImportOptions options = new ImportOptions(args);

        LOG.info("Start Anaplan processing");
        new AnaplanToHDFS(options).start();
    }

    private void start() throws Exception {
        ServiceFactory serviceFactory = new ServiceFactory();
        HttpService http = serviceFactory.createHttpService(options);
        HDFSService hdfs = serviceFactory.createHDFS();
    }
}
