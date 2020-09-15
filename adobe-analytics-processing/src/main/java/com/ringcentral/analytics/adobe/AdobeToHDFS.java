package com.ringcentral.analytics.adobe;

import com.ringcentral.analytics.adobe.service.ExecutionContext;
import com.ringcentral.analytics.adobe.service.HDFSService;
import com.ringcentral.analytics.adobe.service.HttpService;
import com.ringcentral.analytics.adobe.service.JWTTokenService;
import com.ringcentral.analytics.adobe.service.handlers.ServiceFactory;
import com.ringcentral.analytics.adobe.utils.ImportOptions;
import com.ringcentral.analytics.adobe.utils.reports.AdobeReportType;
import com.ringcentral.analytics.adobe.utils.reports.ReportLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Optional;

public class AdobeToHDFS {
   private static final Logger LOG = LoggerFactory.getLogger(AdobeToHDFS.class);
    private final ImportOptions options;

    public AdobeToHDFS(ImportOptions options) {
        this.options = options;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("file.encoding", "UTF-8");
        LOG.info("Parse Adobe Analytics application options");
        //TODO remove empty arg
        if (!Optional.ofNullable(args).isPresent()) {
            args = new String[] {"element1","element2","element3"};;
        }
        ImportOptions options = new ImportOptions(args);

        LOG.info("Start Adobe processing");
        new AdobeToHDFS(options).start();
    }

    private void start() throws Exception {
        ServiceFactory serviceFactory = new ServiceFactory();
        JWTTokenService jwt = serviceFactory.createJWTTokenService(options);
        HttpService http = serviceFactory.createHttpService(options, jwt);
        HDFSService hdfs = serviceFactory.createHDFS();
        Arrays.stream(AdobeReportType.values()).forEach(adobeReportType -> {
            ReportLoader reportLoader = new ReportLoader(http, adobeReportType);
            ExecutionContext context = new ExecutionContext(options, http, hdfs, reportLoader);
            try {
                context.start();
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | IOException e) {
                throw new AdobeException(e.getMessage());
            }
        });
    }
}
