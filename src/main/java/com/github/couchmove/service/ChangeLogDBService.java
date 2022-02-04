package com.github.couchmove.service;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.view.DesignDocument;
import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.*;
import com.github.couchmove.repository.CouchbaseRepository;
import com.github.couchmove.repository.CouchbaseRepositoryImpl;
import lombok.var;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for fetching and executing {@link ChangeLog}s
 *
 * @author ctayeb
 * Created on 03/06/2017
 */
public class ChangeLogDBService {

    private static final Logger logger = LoggerFactory.getLogger(ChangeLogDBService.class);

    public static final String PREFIX_ID = "changelog::";

    private final CouchbaseRepository<ChangeLog> repository;

    public ChangeLogDBService(Bucket bucket, Cluster cluster) {
        this.repository = new CouchbaseRepositoryImpl<>(cluster, bucket, ChangeLog.class);
    }

    public ChangeLogDBService(com.couchbase.client.java.Collection collection, Cluster cluster) {
        this.repository = new CouchbaseRepositoryImpl<>(cluster, collection, ChangeLog.class);
    }

    ChangeLogDBService(CouchbaseRepository<ChangeLog> repository) {
        this.repository = repository;
    }

    /**
     * Get corresponding ChangeLogs from Couchbase bucket
     * <ul>
     * <li>if a {@link ChangeLog} doesn't exist → return it as it its
     * <li>else :
     * <ul>
     * <li>if checksum ({@link ChangeLog#checksum}) is reset (set to null), or description ({@link ChangeLog#description}) updated → reset {@link ChangeLog#cas}
     * <li>return database version
     * </ul>
     * </ul>
     *
     * @param changeLogs to load from database
     * @return database version of changeLogs
     * @throws CouchmoveException if checksum doesn't match
     */
    public List<ChangeLog> fetchAndCompare(List<ChangeLog> changeLogs) {
        logger.info("Reading from bucket '{}'", repository.getBucketName());
        List<ChangeLog> result = new ArrayList<>(changeLogs.size());
        for (ChangeLog changeLog : changeLogs) {
            String version = changeLog.getVersion();
            ChangeLog dbChangeLog = repository.findOne(PREFIX_ID + version);
            if (dbChangeLog == null) {
                logger.debug("Change log version '{}' not found", version);
                result.add(changeLog);
                continue;
            }
            if (dbChangeLog.getChecksum() == null) {
                logger.warn("Change log version '{}' checksum reset", version);
                dbChangeLog.setChecksum(changeLog.getChecksum());
                dbChangeLog.setCas(null);
            } else if (!dbChangeLog.getChecksum().equals(changeLog.getChecksum())) {
                if (dbChangeLog.getStatus() != Status.FAILED) {
                    logger.error("Change log version '{}' checksum doesn't match, please verify if the script '{}' content was modified", changeLog.getVersion(), changeLog.getScript());
                    throw new CouchmoveException("ChangeLog checksum doesn't match");
                }
                dbChangeLog.setStatus(null);
                dbChangeLog.setChecksum(changeLog.getChecksum());
            }
            if (!dbChangeLog.getDescription().equals(changeLog.getDescription())) {
                logger.warn("Change log version '{}' description updated", changeLog.getDescription());
                logger.debug("{} was {}", dbChangeLog, changeLog);
                dbChangeLog.setDescription(changeLog.getDescription());
                dbChangeLog.setScript(changeLog.getScript());
                dbChangeLog.setCas(null);
            }
            result.add(dbChangeLog);
        }
        logger.info("Fetched {} Change logs from bucket", result.size());
        return Collections.unmodifiableList(result);
    }

    /**
     * Saves a {@link ChangeLog} in Couchbase {@link Bucket} using an ID composed by :
     * <p>
     * {@value PREFIX_ID} + {@link ChangeLog#version}
     *
     * @param changeLog The ChangeLog to save
     * @return {@link ChangeLog} entity with CAS (Check And Swap, for optimistic concurrency) set
     */
    public ChangeLog save(ChangeLog changeLog) {
        return repository.save(PREFIX_ID + changeLog.getVersion(), changeLog);
    }

    /**
     * Inserts a {@link DesignDocument} into production
     *
     * @param name    name of the {@link DesignDocument} to insert
     * @param content the content of the {@link DesignDocument} to insert
     */
    public void importDesignDoc(String name, String content) {
        logger.info("Inserting Design Document '{}'...", name);
        repository.importDesignDoc(name, content);
    }

    /**
     * Queries Couchbase {@link Bucket} with multiple N1ql queries
     *
     * @param content containing multiple N1ql queries
     */
    public void executeN1ql(String content) {
        List<String> requests = extractRequests(content);
        logger.info("Executing {} n1ql requests", requests.size());
        requests.forEach(repository::query);
    }

    /**
     * Save multiple json documents to Couchbase {@link Bucket} identified by the keys of the map
     *
     * @param documents a {@link Map} which keys represent a json document to be inserted, and the values the unique ID of the document
     */
    public void importDocuments(Collection<Document> documents) {
        logger.info("Importing {} documents", documents.size());
        for (Document document : documents) {
            var repo = repository;
            if (document.getCollection() != null) {
                repo = repository.withCollection(document.getScope(), document.getCollection());
            }
            repo.save(FilenameUtils.getBaseName(document.getFileName()), document.getContent());
        }
    }

    /**
     * Inserts a Full Text Search Index definition
     *
     * @param name    name of the FTS index to insert
     * @param content the content of the FTS index to insert
     */
    public void importFtsIndex(String name, String content) {
        logger.info("Inserting FTS Index '{}'...", name);
        repository.importFtsIndex(name, content);
    }

    /**
     * Watches all indexes, polling the query service until they become
     * "online" or the timeout has expired
     *
     * @param duration the maximum duration for which to poll for the index to become online.
     */
    public void waitForN1qlIndexes(Duration duration) {
        repository.watchN1qlIndexes(duration);
    }

    /**
     * Watches all indexes, polling the query service until they become
     * "online" or the timeout has expired
     *
     * @param scope scope name.
     * @param duration the maximum duration for which to poll for the index to become online.
     */
    public void waitForN1qlIndexes(String scope, Duration duration) {
        repository.watchN1qlIndexes(scope, duration);
    }

    public void watchN1qlIndexes(String scope, String collection, Duration duration) {
        repository.watchN1qlIndexes(scope, collection, duration);
    }

    /**
     * Instruct the query engine to trigger the build of indexes that have been deferred, within the default management
     */
    public void buildN1qlDeferredIndexes() {
        repository.buildN1qlDeferredIndexes();
    }

    public void buildN1qlDeferredIndexes(String scope, String collection) {
        repository.buildN1qlDeferredIndexes(scope, collection);
    }

    /**
     * Instruct the query engine to trigger the build of indexes of a scope that have been deferred, within the default management
     *
     * @param scope Scope name
     */
    public void buildN1qlDeferredIndexes(String scope) {
        repository.buildN1qlDeferredIndexes(scope);
    }

    /**
     * Extract multiple requests, separated by ';' ignoring :
     * <ul>
     * <li> multi-line (\/* ... *\/) comments
     * <li> unique line (-- ...) comments
     * </ul>
     *
     * @param content content from where the requests are extracted
     * @return multiple requests
     */
    static List<String> extractRequests(String content) {
        String commentsRemoved = content.replaceAll("((?:--[^\\n]*)|(?s)(?:\\/\\*.*?\\*\\/))", "")
                .trim();

        return Arrays.stream(commentsRemoved.split(";"))
                .map(String::trim)
                .collect(Collectors.toList());
    }
}
