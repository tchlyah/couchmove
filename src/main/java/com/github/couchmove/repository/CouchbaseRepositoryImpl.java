package com.github.couchmove.repository;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.RawJsonDocument;
import com.github.couchmove.exception.CouchMoveException;
import com.github.couchmove.pojo.CouchbaseEntity;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.IOException;

/**
 * Created by tayebchlyah on 27/05/2017.
 */
@RequiredArgsConstructor
public class CouchbaseRepositoryImpl<E extends CouchbaseEntity> implements CouchbaseRepository<E> {

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
            bucket.upsert(RawJsonDocument.create(id, getJsonMapper().writeValueAsString(entity)));
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
            if (entity.getCas() != null) {
                bucket.replace(RawJsonDocument.create(id, content, entity.getCas()));
            } else {
                bucket.insert(RawJsonDocument.create(id, content));
            }
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
    public String getBucketName() {
        return bucket.name();
    }
}
