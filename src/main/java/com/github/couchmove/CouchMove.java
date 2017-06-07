package com.github.couchmove;

import com.couchbase.client.java.Bucket;
import com.github.couchmove.exception.CouchMoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.service.ChangeLockService;
import com.github.couchmove.service.ChangeLogDBService;
import com.github.couchmove.service.ChangeLogFileService;
import com.google.common.base.Stopwatch;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.github.couchmove.pojo.Status.*;
import static com.github.couchmove.utils.Utils.getUsername;

/**
 * Created by tayebchlyah on 03/06/2017.
 */
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class CouchMove {

    public static final String DEFAULT_MIGRATION_PATH = "db/migration";

    private static final Logger logger = LoggerFactory.getLogger(CouchMove.class);

    private String bucketName;

    private ChangeLockService lockService;

    private ChangeLogDBService dbService;

    private ChangeLogFileService fileService;

    public CouchMove(Bucket bucket) {
        this(bucket, DEFAULT_MIGRATION_PATH);
    }

    public CouchMove(Bucket bucket, String changePath) {
        logger.info("Connected to bucket '{}'", bucketName = bucket.name());
        lockService = new ChangeLockService(bucket);
        dbService = new ChangeLogDBService(bucket);
        fileService = new ChangeLogFileService(changePath);
    }

    public void migrate() {
        logger.info("Begin bucket '{}' migration", bucketName);
        try {
            // Acquire bucket lock
            if (!lockService.acquireLock()) {
                logger.error("CouchMove did not acquire bucket '{}' lock. Exiting", bucketName);
                throw new CouchMoveException("Unable to acquire lock");
            }

            // Fetching ChangeLogs from migration directory
            List<ChangeLog> changeLogs = fileService.fetch();
            if (changeLogs.isEmpty()) {
                logger.info("CouchMove did not find any migration scripts");
                return;
            }

            // Fetching corresponding ChangeLogs from bucket
            changeLogs = dbService.fetchAndCompare(changeLogs);

            // Executing migration
            executeMigration(changeLogs);
        } finally {
            // Release lock
            lockService.releaseLock();
        }
        logger.info("CouchMove has finished his job");
    }

    void executeMigration(List<ChangeLog> changeLogs) {
        logger.info("Executing migration scripts...");
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
                    logger.info("Updating changeLog '{}'", changeLog.getVersion());
                    dbService.save(changeLog);
                }
                continue;
            }

            if (changeLog.getStatus() == SKIPPED) {
                continue;
            }

            if (lastVersion.compareTo(changeLog.getVersion()) >= 0) {
                logger.warn("ChangeLog '{}' version is lower than last executed one '{}'. Skipping", changeLog.getVersion(), lastVersion);
                changeLog.setStatus(SKIPPED);
                dbService.save(changeLog);
                continue;
            }

            if (executeMigration(changeLog)) {
                changeLog.setOrder(++lastOrder);
                lastVersion = changeLog.getVersion();
                migrationCount++;
            } else {
                throw new CouchMoveException("Migration failed");
            }
        }
        if (migrationCount == 0) {
            logger.info("No new migration scripts found");
        } else {
            logger.info("Executed {} migration scripts", migrationCount);
        }
    }

    boolean executeMigration(ChangeLog changeLog) {
        logger.info("Executing ChangeLog '{}'", changeLog.getVersion());
        Stopwatch sw = Stopwatch.createStarted();
        changeLog.setTimestamp(new Date());
        changeLog.setRunner(getUsername());
        if (doExecute(changeLog)) {
            logger.info("ChangeLog '{}' successfully executed", changeLog.getVersion());
            changeLog.setStatus(EXECUTED);
        } else {
            logger.error("Unable to execute ChangeLog '{}'", changeLog.getVersion());
            changeLog.setStatus(FAILED);
        }
        changeLog.setDuration(sw.elapsed(TimeUnit.MILLISECONDS));
        dbService.save(changeLog);
        return changeLog.getStatus() == EXECUTED;
    }

    private boolean doExecute(ChangeLog changeLog) {
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
            return true;
        } catch (Exception e) {
            logger.error("Unable to import " + changeLog.getType().name().toLowerCase().replace("_", " ") + " : '" + changeLog.getScript() + "'", e);
            return false;
        }
    }
}

