package com.github.couchmove;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.view.DesignDocument;
import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.pojo.Status;
import com.github.couchmove.pojo.Type;
import com.github.couchmove.pojo.Type.Constants;
import com.github.couchmove.service.ChangeLockService;
import com.github.couchmove.service.ChangeLogDBService;
import com.github.couchmove.service.ChangeLogFileService;
import com.github.couchmove.utils.Utils;
import com.google.common.base.Stopwatch;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.github.couchmove.pojo.Status.*;
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

    private String bucketName;

    private ChangeLockService lockService;

    @Setter(AccessLevel.PACKAGE)
    private ChangeLogDBService dbService;

    private ChangeLogFileService fileService;

    /**
     * Initialize a {@link Couchmove} instance with default migration path : {@value DEFAULT_MIGRATION_PATH}
     *
     * @param bucket Couchbase {@link Bucket} to execute the migrations on
     */
    public Couchmove(Bucket bucket) {
        this(bucket, DEFAULT_MIGRATION_PATH);
    }

    /**
     * Initialize a {@link Couchmove} instance
     *
     * @param bucket     Couchbase {@link Bucket} to execute the migrations on
     * @param changePath absolute or relative path of the migration folder containing {@link ChangeLog}
     */
    public Couchmove(Bucket bucket, String changePath) {
        logger.info("Connected to bucket '{}'", bucketName = bucket.name());
        lockService = new ChangeLockService(bucket);
        dbService = new ChangeLogDBService(bucket);
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
        logger.info("Begin bucket '{}' update", bucketName);
        try {
            // Acquire bucket lock
            if (!lockService.acquireLock()) {
                logger.error("Couchmove did not acquire bucket '{}' change log lock. Exiting...", bucketName);
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
        String lastVersion = "";
        int lastOrder = 0;
        Optional<ChangeLog> lastExecutedChangeLog = changeLogs.stream()
                .filter(c -> c.getStatus() == EXECUTED)
                .max(Comparator.naturalOrder());
        if (lastExecutedChangeLog.isPresent()) {
            lastVersion = lastExecutedChangeLog.get().getVersion();
            lastOrder = lastExecutedChangeLog.get().getOrder();
        }

        for (ChangeLog changeLog : changeLogs) {
            if (changeLog.getStatus() == EXECUTED) {
                lastVersion = changeLog.getVersion();
                lastOrder = changeLog.getOrder();
            }
        }

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

            if (lastVersion.compareTo(changeLog.getVersion()) >= 0) {
                logger.warn("ChangeLog '{}::{}' version is lower than last executed one '{}'. Skipping", changeLog.getVersion(), changeLog.getDescription(), lastVersion);
                changeLog.setStatus(SKIPPED);
                dbService.save(changeLog);
                continue;
            }

            executeMigration(changeLog, lastOrder + 1);
            lastOrder++;
            lastVersion = changeLog.getVersion();
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
     * @param order the order to set if the execution was successful
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
     * Applies the {@link ChangeLog} according to it's {@link ChangeLog#type} :
     * <ul>
     *     <li> {@link Type#DOCUMENTS} : Imports all {@value Constants#JSON} documents contained in the folder
     *     <li> {@link Type#N1QL} : Execute all {@link N1qlQuery} contained in the {@value Constants#N1QL} file
     *     <li> {@link Type#DESIGN_DOC} : Imports {@link DesignDocument} contained in the {@value Constants#JSON} document
     * </ul>
     *
     * @param changeLog {@link ChangeLog} to apply
     * @throws CouchmoveException if the execution fail
     */
    void doExecute(ChangeLog changeLog) {
        try {
            switch (changeLog.getType()) {
                case DOCUMENTS:
                    dbService.importDocuments(fileService.readDocuments(changeLog.getScript()));
                    break;
                case N1QL:
                    dbService.executeN1ql(fileService.readFile(changeLog.getScript()));
                    break;
                case DESIGN_DOC:
                    dbService.importDesignDoc(changeLog.getDescription().replace(" ", "_"), fileService.readFile(changeLog.getScript()));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown ChangeLog Type '" + changeLog.getType() + "'");
            }
        } catch (Exception e) {
            throw new CouchmoveException("Unable to import " + changeLog.getType().name().toLowerCase().replace("_", " ") + " : '" + changeLog.getScript() + "'", e);
        }
    }
}

