package com.github.couchmove;

import ch.qos.logback.classic.*;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.*;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.*;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@Command(name = "couchmove", mixinStandardHelpOptions = true, version = "Couchmove 1.0")
public class CouchmoveCommand implements Runnable {

    @Option(names = {"-U", "--URL"}, required = true, description = "The URL of the Couchbase cluster")
    private String url;
    @Option(names = {"-u", "--username"}, required = true, description = "The username for the Couchbase cluster")
    private String username;

    @Option(names = {"-p", "--password"}, required = true, description = "The password for the Couchbase cluster")
    private String password;

    @Option(names = {"-b", "--bucket"}, required = true, description = "The name of the Couchbase bucket")
    private String bucket;

    @Option(names = {"-S", "--scope"}, defaultValue = "_default", description = "The scope to use")
    private String scopeName;

    @Option(names = {"-C", "--collection"}, defaultValue = "_default", description = "The collection to use")
    private String collectionName;

    @Option(names = {"-c", "--change-log-path"}, required = true, description = "The path to the change log file")
    private String changeLogPath;

    @Option(names = "--build-n1ql-indexes", arity = "0..2", paramLabel = "[scopeName[.collectionName]]", hideParamSyntax = true, split = "\\.", description = "Build N1QL Deferred indexes.\nExamples:\n\t--build-n1ql-indexes : Build indexes on the previous scope and collection parameters\n\t--build-n1ql-indexes scopeName : Build all indexes under 'scopeName' scope\n\t--build-n1ql-indexes scopeName.collectionName : Build all indexes under 'scopeName' scope and 'collectionName' collection")
    private List<String> buildN1qlIndexes;

    @Option(names = {"-V", "--variable"}, split = ",", description = "Custom variables in key=value format (e.g. -V key1=value1,key2=value2)")
    private final Map<String, String> customVariables = new HashMap<>();

    @Option(names = { "-v", "--verbose" }, description = {
            "Specify multiple -v options to increase verbosity. Maximum 2",
            "For example, `-v -v` or `-vv`" })
    private final boolean[] verbosity = new boolean[0];

    public static void main(String[] args) {
        int exitCode = new CommandLine(new CouchmoveCommand()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger(ROOT_LOGGER_NAME);
        if (verbosity.length == 0) {
            logger.setLevel(Level.INFO);
        } else if (verbosity.length >= 2) {
            logger.setLevel(Level.TRACE);
        }

        Cluster cluster = Cluster.connect(url, username, password);
        Scope scope = cluster.bucket(bucket).scope(scopeName);
        Collection collection = scope.collection(collectionName);

        Couchmove couchmove = new Couchmove(collection, cluster, changeLogPath, customVariables);
        couchmove.migrate();
        if (buildN1qlIndexes != null) {
            if (buildN1qlIndexes.isEmpty()) {
                couchmove.buildN1qlDeferredIndexes();
            } else if (buildN1qlIndexes.size() == 1) {
                couchmove.buildN1qlDeferredIndexes(buildN1qlIndexes.get(0));
            } else {
                couchmove.buildN1qlDeferredIndexes(buildN1qlIndexes.get(0), buildN1qlIndexes.get(1));
            }
        }
    }
}
