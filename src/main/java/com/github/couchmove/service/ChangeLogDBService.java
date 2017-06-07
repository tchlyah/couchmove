package com.github.couchmove.service;

import com.couchbase.client.java.Bucket;
import com.github.couchmove.exception.CouchMoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.repository.CouchbaseRepository;
import com.github.couchmove.repository.CouchbaseRepositoryImpl;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.github.couchmove.utils.FunctionUtils.not;

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

    /**
     * Get corresponding ChangeLogs from Couchbase bucket
     * <ul>
     * <li>if a {@link ChangeLog} doesn't exist => return it as it its
     * <li>else :
     * <ul>
     * <li>if checksum ({@link ChangeLog#checksum}) is reset (set to null), or description ({@link ChangeLog#description}) updated => reset {@link ChangeLog#cas}
     * <li>return database version
     * </ul>
     * </ul>
     *
     * @param changeLogs to load from database
     * @return database version of changeLogs
     * @throws CouchMoveException if checksum doesn't match
     */
    public List<ChangeLog> fetchAndCompare(List<ChangeLog> changeLogs) {
        logger.info("Fetching changeLogs from bucket '{}'", repository.getBucketName());
        List<ChangeLog> result = new ArrayList<>(changeLogs.size());
        for (ChangeLog changeLog : changeLogs) {
            String version = changeLog.getVersion();
            ChangeLog dbChangeLog = repository.findOne(PREFIX_ID + version);
            if (dbChangeLog == null) {
                logger.debug("ChangeLog version '{}' not found", version);
                result.add(changeLog);
                continue;
            }
            if (dbChangeLog.getChecksum() == null) {
                logger.warn("ChangeLog version '{}' checksum reset");
                dbChangeLog.setChecksum(changeLog.getChecksum());
                dbChangeLog.setCas(null);
            } else if (!dbChangeLog.getChecksum().equals(changeLog.getChecksum())) {
                logger.error("ChangeLog version '{}' checksum doesn't match, please verify if the script '{}' content was modified", changeLog.getVersion(), changeLog.getScript());
                throw new CouchMoveException("ChangeLog checksum doesn't match");
            }
            if (!dbChangeLog.getDescription().equals(changeLog.getDescription())) {
                logger.warn("ChangeLog version '{}' description updated");
                logger.debug("{} was {}", dbChangeLog, changeLog);
                dbChangeLog.setDescription(changeLog.getDescription());
                dbChangeLog.setScript(changeLog.getScript());
                dbChangeLog.setCas(null);
            }
            result.add(dbChangeLog);
        }
        logger.info("Fetched {} changeLogs from bucket", result.size());
        return Collections.unmodifiableList(result);
    }

    public ChangeLog save(ChangeLog changeLog) {
        return repository.save(PREFIX_ID + changeLog.getVersion(), changeLog);
    }

    public void importDesignDoc(String name, String content) {
        logger.info("Inserting Design Document '{}'...", name);
        repository.importDesignDoc(name, content);
    }

    public void executeN1ql(String content) {
        List<String> requests = extractRequests(content);
        logger.info("Executing {} n1ql requests", requests.size());
        requests.forEach(repository::query);
    }

    public void importDocuments(Map<String, String> documents) {
        logger.info("Importing {} documents", documents.size());
        documents.forEach((fileName, content) ->
                repository.save(FilenameUtils.getBaseName(fileName), content));
    }

    static List<String> extractRequests(String content) {
        String commentsRemoved = content.replaceAll("((?:--[^\\n]*)|(?s)(?:\\/\\*.*?\\*\\/))", "")
                .trim();

        return Arrays.stream(commentsRemoved.split(";"))
                .map(String::trim)
                .collect(Collectors.toList());
    }
}
