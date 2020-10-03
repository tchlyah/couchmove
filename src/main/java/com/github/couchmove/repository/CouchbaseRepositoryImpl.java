package com.github.couchmove.repository;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.search.GetSearchIndexRequest;
import com.couchbase.client.core.message.search.GetSearchIndexResponse;
import com.couchbase.client.core.message.search.UpsertSearchIndexRequest;
import com.couchbase.client.core.message.search.UpsertSearchIndexResponse;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.util.IndexInfo;
import com.couchbase.client.java.util.retry.RetryBuilder;
import com.couchbase.client.java.util.retry.RetryWhenFunction;
import com.couchbase.client.java.view.DesignDocument;
import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.CouchbaseEntity;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import rx.Observable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.couchbase.client.java.query.consistency.ScanConsistency.STATEMENT_PLUS;
import static com.google.common.collect.ImmutableMap.of;
import static lombok.AccessLevel.PACKAGE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author ctayeb
 * Created on 27/05/2017
 */
// For tests
@SuppressWarnings("ConstantConditions")
@NoArgsConstructor(access = PACKAGE, force = true)
@RequiredArgsConstructor
public class CouchbaseRepositoryImpl<E extends CouchbaseEntity> implements CouchbaseRepository<E> {

    private static final Logger logger = getLogger(CouchbaseRepositoryImpl.class);

    @Getter(lazy = true)
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    public static final String BUCKET_PARAM = "bucket";

    private final Bucket bucket;

    private final String username;

    private final String password;

    private final Class<E> entityClass;

    @Getter(lazy = true, value = AccessLevel.PRIVATE)
    private static final RetryWhenFunction retryStrategy = retryStrategy();

    @Override
    public E save(String id, E entity) {
        logger.trace("Save entity '{}' with id '{}'", entity, id);
        try {
            String json = getJsonMapper().writeValueAsString(entity);
            RawJsonDocument insertedDocument = runAsync(bucket -> bucket.upsert(RawJsonDocument.create(id, json)));
            entity.setCas(insertedDocument.cas());
            return entity;
        } catch (JsonProcessingException e) {
            throw new CouchmoveException("Unable to save document with id " + id, e);
        }
    }

    @Override
    public E checkAndSave(String id, E entity) {
        logger.trace("Check and save entity '{}' with id '{}'", entity, id);
        try {
            String content = getJsonMapper().writeValueAsString(entity);
            RawJsonDocument insertedDocument;
            insertedDocument = runAsync(bucket -> {
                if (entity.getCas() != null) {
                    return bucket.replace(RawJsonDocument.create(id, content, entity.getCas()));
                }
                return bucket.insert(RawJsonDocument.create(id, content));
            });
            entity.setCas(insertedDocument.cas());
            return entity;
        } catch (JsonProcessingException e) {
            throw new CouchmoveException("Unable to save document with id " + id, e);
        }
    }

    @Override
    public void delete(String id) {
        logger.trace("Remove entity with id '{}'", id);
        try {
            runAsync(bucket -> bucket.remove(id));
        } catch (DocumentDoesNotExistException e) {
            logger.debug("Trying to delete document that does not exist : '{}'", id);
        }
    }

    @Override
    public E findOne(String id) {
        logger.trace("Find entity with id '{}'", id);
        RawJsonDocument document = runAsync(bucket -> bucket.get(id, RawJsonDocument.class));
        if (document == null) {
            return null;
        }
        try {
            E entity = getJsonMapper().readValue(document.content(), entityClass);
            entity.setCas(document.cas());
            return entity;
        } catch (IOException e) {
            throw new CouchmoveException("Unable to read document with id " + id, e);
        }
    }

    @Override
    public void save(String id, String jsonContent) {
        logger.trace("Save document with id '{}' : \n'{}'", id, jsonContent);
        runAsync(bucket -> bucket.upsert(RawJsonDocument.create(id, jsonContent)));
    }

    @Override
    public void importDesignDoc(String name, String jsonContent) {
        logger.trace("Import document : \n'{}'", jsonContent);
        bucket.bucketManager().upsertDesignDocument(DesignDocument.from(name, JsonObject.fromJson(jsonContent)));
    }

    @Override
    public void query(String n1qlStatement) {
        String parametrizedStatement = injectParameters(n1qlStatement);
        logger.debug("Execute n1ql request : \n{}", parametrizedStatement);
        try {
            AsyncN1qlQueryResult result = runAsync(bucket -> bucket
                    .query(N1qlQuery.simple(parametrizedStatement,
                            N1qlParams.build().consistency(STATEMENT_PLUS))));
            if (!result.parseSuccess()) {
                logger.error("Invalid N1QL request '{}' : {}", parametrizedStatement, single(result.errors()));
                throw new CouchmoveException("Invalid n1ql request");
            }
            if (!single(result.finalSuccess())) {
                logger.error("Unable to execute n1ql request '{}'. Status : {}, errors : {}", parametrizedStatement, single(result.status()), single(result.errors()));
                throw new CouchmoveException("Unable to execute n1ql request");
            }
        } catch (Exception e) {
            throw new CouchmoveException("Unable to execute n1ql request", e);
        }
    }

    @Override
    public void importFtsIndex(String name, String jsonContent) {
        jsonContent = injectParameters(jsonContent);
        logger.trace("Import FTS index : \n'{}'", jsonContent);
        UpsertSearchIndexResponse response = bucket.core()
                .send(new UpsertSearchIndexRequest(name, jsonContent, username, password))
                .map(UpsertSearchIndexResponse.class::cast)
                .toBlocking()
                .single();

        if (!response.status().isSuccess()) {
            String msg = response.payload();
            throw new CouchmoveException("Could not store FTS index : " + msg);
        }
    }

    @Override
    public boolean isFtsIndexExists(String name) {
        GetSearchIndexResponse response = bucket.core()
                .send(new GetSearchIndexRequest(name, username, password))
                .map(GetSearchIndexResponse.class::cast)
                .toBlocking()
                .single();
        return response.status() == ResponseStatus.EXISTS || response.status() == ResponseStatus.SUCCESS;
    }

    String injectParameters(String statement) {
        return StrSubstitutor.replace(statement, of(BUCKET_PARAM, getBucketName()));
    }

    @Override
    public String getBucketName() {
        return bucket.name();
    }

    //<editor-fold desc="Helpers">
    @Nullable
    private <T> T single(Observable<T> observable) {
        return observable.toBlocking().singleOrDefault(null);
    }

    private <R> R runAsync(Function<AsyncBucket, Observable<R>> function) {
        return single(function.apply(bucket.async())
                .retryWhen(getRetryStrategy()));
    }

    @SuppressWarnings("unchecked")
    private static RetryWhenFunction retryStrategy() {
        return RetryBuilder
                .anyOf(TemporaryFailureException.class, RequestCancelledException.class, BackpressureException.class)
                .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100))
                .max(3)
                .build();
    }
    //</editor-fold>
}
