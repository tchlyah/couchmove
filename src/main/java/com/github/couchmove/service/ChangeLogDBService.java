package com.github.couchmove.service;

import com.couchbase.client.java.Bucket;
import com.github.couchmove.exception.CouchMoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.repository.CouchbaseRepository;
import com.github.couchmove.repository.CouchbaseRepositoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by tayebchlyah on 03/06/2017.
 */
public class ChangeLogDBService {

    private static final Logger logger = LoggerFactory.getLogger(ChangeLogDBService.class);

    public static final String PREFIX_ID = "changelog::";

    private CouchbaseRepository<ChangeLog> repository;

    public ChangeLogDBService(Bucket bucket) {
        this.repository = new CouchbaseRepositoryImpl<>(bucket, ChangeLog.class);
    }

    public List<ChangeLog> fetchAndCompare(List<ChangeLog> changeLogs) {
        logger.info("Fetching changeLogs from bucket '{}'", repository.getBucketName());
        List<ChangeLog> result = new ArrayList<>(changeLogs.size());
        for (ChangeLog changeLog : changeLogs) {
            String version = changeLog.getVersion();
            ChangeLog dbChangeLog = repository.findOne(PREFIX_ID + version);
            if (dbChangeLog == null) {
                logger.debug("ChangeLog version '{}' not found", version);
                result.add(changeLog);
            } else if (dbChangeLog.getChecksum() != null && !dbChangeLog.getChecksum().equals(changeLog.getChecksum())) {
                logger.error("ChangeLog version '{}' checksum doesn't match, please verify if the script '{}' content was modified", changeLog.getVersion(), changeLog.getScript());
                throw new CouchMoveException("ChangeLog checksum doesn't match");
            } else {
                logger.warn("ChangeLog version '{}' description updated");
                logger.debug("{} was {}", dbChangeLog, changeLog);
                dbChangeLog.setDescription(changeLog.getDescription());
                dbChangeLog.setScript(changeLog.getScript());
                result.add(dbChangeLog);
            }
        }
        logger.info("Fetched {} changeLogs from bucket", result.size());
        return Collections.unmodifiableList(result);
    }

    public String getBucketName() {
        return repository.getBucketName();
    }
}
