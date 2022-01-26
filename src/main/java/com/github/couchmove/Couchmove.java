package com.github.couchmove;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.*;
import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.*;
import com.github.couchmove.pojo.Type.Constants;
import com.github.couchmove.service.*;
import com.github.couchmove.utils.Utils;
import com.google.common.base.Stopwatch;
import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.github.couchmove.pojo.Status.*;
import static com.github.couchmove.pojo.Type.DOCUMENTS;
import static com.github.couchmove.utils.Utils.elapsed;
import static java.lang.String.format;

/**
 * Couchmove Runner
 *
 * @author ctayeb
 * Created on 03/06/2017
 */
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class Couchmove {

    public static final String DEFAULT_MIGRATION_PATH = "db/migration";

    private static final Logger logger = LoggerFactory.getLogger(Couchmove.class);

    private String collectionOrBucketName;

    private ChangeLockService lockService;

    @Setter(AccessLevel.PACKAGE)
    private ChangeLogDBService dbService;

    private ChangeLogFileService fileService;

    /**
     * Initialize a {@link Couchmove} instance with default migration path : {@value DEFAULT_MIGRATION_PATH}
     *
     * @param bucket  Couchbase {@link Bucket} to execute the migrations on
     * @param cluster Couchbase {@link Cluster} to execute N1ql Requets and insert FTS indexes
     */
    public Couchmove(Bucket bucket, Cluster cluster) {
        this(bucket, cluster, DEFAULT_MIGRATION_PATH);
    }

    /**
     * Initialize a {@link Couchmove} instance
     *
     * @param bucket     Couchbase {@link Bucket} to execute the migrations on
     * @param cluster    Couchbase {@link Cluster} to execute N1ql Requests and insert FTS indexes
     * @param changePath absolute or relative path of the migration folder containing {@link ChangeLog}
     */
    public Couchmove(Bucket bucket, Cluster cluster, String changePath) {
        logger.info("Connected to bucket '{}'", collectionOrBucketName = bucket.name());
        lockService = new ChangeLockService(bucket, cluster);
        dbService = new ChangeLogDBService(bucket, cluster);
        fileService = new ChangeLogFileService(changePath);
    }

    /**
     * Initialize a {@link Couchmove} instance with default migration path : {@value DEFAULT_MIGRATION_PATH}
     *
     * @param collection Couchbase {@link Collection} to execute the migrations on
     * @param cluster    Couchbase {@link Cluster} to execute N1ql Requets and insert FTS indexes
     */
    public Couchmove(Collection collection, Cluster cluster) {
        this(collection, cluster, DEFAULT_MIGRATION_PATH);
    }

    /**
     * Initialize a {@link Couchmove} instance
     *
     * @param collection Couchbase {@link Collection} to execute the migrations on
     * @param cluster    Couchbase {@link Cluster} to execute N1ql Requets and insert FTS indexes
     * @param changePath absolute or relative path of the migration folder containing {@link ChangeLog}
     */
    public Couchmove(Collection collection, Cluster cluster, String changePath) {
        logger.info("Connected to collection '{}'", collectionOrBucketName = collection.name());
        lockService = new ChangeLockService(collection, cluster);
        dbService = new ChangeLogDBService(collection, cluster);
        fileService = new ChangeLogFileService(changePath);
    }

    /**
     * Launch the migration process :
     * <ol>
     *     <li> Tries to acquire Couchbase {@link Bucket} lock
     *     <li> Fetch all {@link ChangeLog}s from migration folder
     *     <li> Fetch corresponding {@link ChangeLog}s from {@link Bucket}
     *     <li> Execute found {@link ChangeLog}s : {@link Couchmove#executeMigration(List)}
     * </ol>
     *
     * @throws CouchmoveException if migration fail
     */
    public void migrate() throws CouchmoveException {
        logger.info("Begin '{}' update", collectionOrBucketName);
        try {
            // Acquire bucket lock
            if (!lockService.acquireLock()) {
                logger.error("Couchmove did not acquire '{}' change log lock. Exiting...", collectionOrBucketName);
                throw new CouchmoveException("Unable to acquire lock");
            }

            // Fetching ChangeLogs from migration directory
            List<ChangeLog> changeLogs = fileService.fetch();
            if (changeLogs.isEmpty()) {
                logger.info("Couchmove did not find any change logs");
                return;
            }

            // Fetching corresponding ChangeLogs from bucket
            changeLogs = dbService.fetchAndCompare(changeLogs);

            // Executing migration
            executeMigration(changeLogs);
        } catch (Exception e) {
            logger.error("Couchmove Update failed");
            throw new CouchmoveException("Unable to migrate", e);
        } finally {
            // Release lock
            lockService.releaseLock();
        }
        logger.info("Couchmove Update Successful");
    }

    /**
     * Execute the {@link ChangeLog}s
     * <ul>
     *      <li> If {@link ChangeLog#version} is lower than last executed one, ignore it and mark it as {@link Status#SKIPPED}
     *      <li> If an {@link Status#EXECUTED} ChangeLog was modified, fail
     *      <li> If an {@link Status#EXECUTED} ChangeLog description was modified, update it
     *      <li> Otherwise apply the ChangeLog : {@link Couchmove#executeMigration(ChangeLog, int)}
     *  </ul>
     *
     * @param changeLogs to execute
     */
    void executeMigration(List<ChangeLog> changeLogs) {
        logger.info("Applying change logs...");
        int migrationCount = 0;
        // Get version and order of last executed changeLog
        ChangeLog lastExecutedChangeLog = changeLogs.stream()
                .filter(c -> c.getStatus() == EXECUTED)
                .max(Comparator.naturalOrder())
                .orElse(ChangeLog.builder().order(0).build());

        for (ChangeLog changeLog : changeLogs) {
            if (changeLog.getStatus() == EXECUTED) {
                if (changeLog.getCas() == null) {
                    logger.info("Updating change log '{}::{}'", changeLog.getVersion(), changeLog.getDescription());
                    dbService.save(changeLog);
                }
                continue;
            }

            if (changeLog.getStatus() == SKIPPED) {
                continue;
            }

            if (changeLog.compareTo(lastExecutedChangeLog) <= 0) {
                logger.warn("ChangeLog '{}::{}' version is lower than last executed one '{}'. Skipping", changeLog.getVersion(), changeLog.getDescription(), lastExecutedChangeLog.getVersion());
                changeLog.setStatus(SKIPPED);
                dbService.save(changeLog);
                continue;
            }

            executeMigration(changeLog, lastExecutedChangeLog.getOrder() + 1);
            lastExecutedChangeLog = changeLog;
            migrationCount++;
        }
        if (migrationCount == 0) {
            logger.info("No new change logs found");
        } else {
            logger.info("Applied {} change logs", migrationCount);
        }
    }

    /**
     * Execute the migration {@link ChangeLog}, and save it to Couchbase {@link Bucket}
     * <ul>
     *     <li> If the execution was successful, set the order and mark it as {@link Status#EXECUTED}
     *     <li> Otherwise, mark it as {@link Status#FAILED}
     * </ul>
     *
     * @param changeLog {@link ChangeLog} to execute
     * @param order     the order to set if the execution was successful
     * @throws CouchmoveException if the execution fail
     */
    void executeMigration(ChangeLog changeLog, int order) {
        logger.info("Applying change log '{}::{}'", changeLog.getVersion(), changeLog.getDescription());
        Stopwatch sw = Stopwatch.createStarted();
        changeLog.setTimestamp(new Date());
        changeLog.setRunner(Utils.getUsername());
        try {
            doExecute(changeLog);
            logger.info("Change log '{}::{}' ran successfully in {}", changeLog.getVersion(), changeLog.getDescription(), elapsed(sw));
            changeLog.setOrder(order);
            changeLog.setStatus(EXECUTED);
        } catch (CouchmoveException e) {
            changeLog.setStatus(FAILED);
            throw new CouchmoveException(format("Unable to apply change log '%s::%s'", changeLog.getVersion(), changeLog.getDescription()), e);
        } finally {
            changeLog.setDuration(sw.elapsed(TimeUnit.MILLISECONDS));
            dbService.save(changeLog);
        }
    }

    /**
     * Instruct the query engine to trigger the build of indexes that have been deferred, within the default management
     */
    public void buildN1qlDeferredIndexes() {
        dbService.buildN1qlDeferredIndexes();
    }

    /**
     * Instruct the query engine to trigger the build of indexes that have been deferred, within the default management
     *
     * @param scope      {@link Scope} name
     * @param collection {@link Collection} name
     */
    public void buildN1qlDeferredIndexes(String scope, String collection) {
        dbService.buildN1qlDeferredIndexes(scope, collection);
    }

    /**
     * Instruct the query engine to trigger the build of indexes of a scope that have been deferred, within the default management
     *
     * @param scope {@link Scope} name
     */
    public void buildN1qlDeferredIndexes(String scope) {
        dbService.buildN1qlDeferredIndexes(scope);
    }

    /**
     * Watches all indexes, polling the query service until they become
     * "online" or the timeout has expired
     */
    public void waitForN1qlIndexes(Duration duration) {
        dbService.waitForN1qlIndexes(duration);
    }

    /**
     * Watches all indexes, polling the query service until they become
     * "online" or the timeout has expired
     *
     * @param scope    {@link Scope} name.
     * @param duration the maximum duration for which to poll for the index to become online.
     */
    public void waitForN1qlIndexes(String scope, Duration duration) {
        dbService.waitForN1qlIndexes(scope, duration);
    }

    /**
     * Watches all indexes, polling the query service until they become
     * "online" or the timeout has expired
     *
     * @param scope      {@link Scope} name.
     * @param collection {@link Collection} name.
     * @param duration   the maximum duration for which to poll for the index to become online.
     */
    public void waitForN1qlIndexes(String scope, String collection, Duration duration) {
        dbService.watchN1qlIndexes(scope, collection, duration);
    }

    /**
     * Applies the {@link ChangeLog} according to it's {@link ChangeLog#type} :
     * <ul>
     *     <li> {@link Type#DOCUMENTS} : Imports all {@value Constants#JSON} documents contained in the folder
     *     <li> {@link Type#N1QL} : Execute all N1ql query contained in the {@value Constants#N1QL} file
     *     <li> {@link Type#DESIGN_DOC} : Imports {@link com.couchbase.client.java.manager.view.DesignDocument} contained in the {@value Constants#JSON} document
     *     <li> {@link Type#FTS} : Imports Full Text Search index definition contained in the {@value Constants#FTS} document
     * </ul>
     *
     * @param changeLog {@link ChangeLog} to apply
     * @throws CouchmoveException if the execution fail
     */
    void doExecute(ChangeLog changeLog) {
        Type type = changeLog.getType();
        try {
            if (type == DOCUMENTS) {
                dbService.importDocuments(fileService.readDocuments(changeLog.getScript()));
            } else {
                var description = changeLog.getDescription().replace(" ", "_");
                var content = fileService.readFile(changeLog.getScript());
                switch (type) {
                    case N1QL:
                        dbService.executeN1ql(content);
                        return;
                    case DESIGN_DOC:
                        dbService.importDesignDoc(description, content);
                        return;
                    case FTS:
                        dbService.importFtsIndex(description, content);
                        return;
                    case EVENTING:
                        dbService.importEventingFunctions(description, content);
                        return;
                    default:
                        throw new IllegalArgumentException("Unknown ChangeLog Type '" + type + "'");
                }
            }
        } catch (Exception e) {
            throw new CouchmoveException("Unable to import " + type.name().toLowerCase().replace("_", " ") + " : '" + changeLog.getScript() + "'", e);
        }
    }
}

