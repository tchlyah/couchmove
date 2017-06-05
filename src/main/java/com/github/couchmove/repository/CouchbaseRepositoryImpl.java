package com.github.couchmove.repository;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.view.DesignDocument;
import com.github.couchmove.exception.CouchMoveException;
import com.github.couchmove.pojo.CouchbaseEntity;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;

import java.io.IOException;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by tayebchlyah on 27/05/2017.
 */
// For tests
@NoArgsConstructor(access = AccessLevel.PACKAGE,force = true)
@RequiredArgsConstructor
public class CouchbaseRepositoryImpl<E extends CouchbaseEntity> implements CouchbaseRepository<E> {

    private static final Logger logger = getLogger(CouchbaseRepositoryImpl.class);

    @Getter(lazy = true)
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private final Bucket bucket;

    private final Class<E> entityClass;

    /**
     * Save the entity on couchbase
     *
     * @param id     document id
     * @param entity entity to save
     * @return saved entity
     */
    @Override
    public E save(String id, E entity) {
        try {
            RawJsonDocument insertedDocument = bucket.upsert(RawJsonDocument.create(id, getJsonMapper().writeValueAsString(entity)));
            entity.setCas(insertedDocument.cas());
            return entity;
        } catch (JsonProcessingException e) {
            throw new CouchMoveException("Unable to save document with id " + id, e);
        }
    }

    /**
     * If the {@link CouchbaseEntity#cas} of the entity is set, tries to replace the document with a Check And Set operation (Optimistic locking)
     * </p>
     * otherwise it insert the document
     *
     * @param id     document id
     * @param entity entity to save
     * @return saved entity
     * @throws com.couchbase.client.java.error.CASMismatchException           if the cas of entity is different from existing one
     * @throws com.couchbase.client.java.error.DocumentAlreadyExistsException if the cas is not set and the document exists on couchbase
     */
    @Override
    public E checkAndSave(String id, E entity) {
        try {
            String content = getJsonMapper().writeValueAsString(entity);
            RawJsonDocument insertedDocument;
            if (entity.getCas() != null) {
                insertedDocument = bucket.replace(RawJsonDocument.create(id, content, entity.getCas()));
            } else {
                insertedDocument = bucket.insert(RawJsonDocument.create(id, content));
            }
            entity.setCas(insertedDocument.cas());
            return entity;
        } catch (JsonProcessingException e) {
            throw new CouchMoveException("Unable to save document with id " + id, e);
        }
    }

    @Override
    public void delete(String id) {
        bucket.remove(id);
    }

    @Override
    public E findOne(String id) {
        RawJsonDocument document = bucket.get(id, RawJsonDocument.class);
        if (document == null) {
            return null;
        }
        try {
            E entity = getJsonMapper().readValue(document.content(), entityClass);
            entity.setCas(document.cas());
            return entity;
        } catch (IOException e) {
            throw new CouchMoveException("Unable to read document with id " + id, e);
        }
    }

    @Override
    public void save(String id, String jsonContent) {
        bucket.upsert(RawJsonDocument.create(id, jsonContent));
    }

    @Override
    public void importDesignDoc(String name, String jsonContent) {
        bucket.bucketManager().upsertDesignDocument(DesignDocument.from(name, JsonObject.fromJson(jsonContent)));
    }

    @Override
    public void query(String n1qlStatement) {
        logger.debug("Executing n1ql request : {}", n1qlStatement);
        try {
            N1qlQueryResult result = bucket.query(N1qlQuery.simple(n1qlStatement));
            if (!result.parseSuccess()) {
                logger.info("Invalid N1QL request '{}'", n1qlStatement);
                throw new CouchMoveException("Invalid n1ql request");
            }
            if (!result.finalSuccess()) {
                logger.error("Unable to execute n1ql request '{}'. Status : {}, errors : ", n1qlStatement, result.status(), result.errors());
                throw new CouchMoveException("Unable to execute n1ql request");
            }
        } catch (Exception e) {
            throw new CouchMoveException("Unable to execute n1ql request", e);
        }
    }

    @Override
    public String getBucketName() {
        return bucket.name();
    }
}
