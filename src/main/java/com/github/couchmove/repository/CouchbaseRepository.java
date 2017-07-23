package com.github.couchmove.repository;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.view.DesignDocument;
import com.github.couchmove.pojo.CouchbaseEntity;

/**
 * A repository for encapsulating storage, retrieval, and removal of json documents to Couchbase {@link Bucket}
 *
 * @param <E> the domain type the repository manages
 * @author ctayeb
 * Created on 27/05/2017
 */
public interface CouchbaseRepository<E extends CouchbaseEntity> {

    /**
     * Convert an {@link CouchbaseEntity} to json document, and save it to Couchbase {@link Bucket}
     *
     * @param id     the per-bucket unique document id
     * @param entity entity to convert and save
     * @return saved entity with CAS (Compare and Swap) for optimistic concurrency
     */
    E save(String id, E entity);

    /**
     * If the {@link CouchbaseEntity#cas} of the entity is set, tries to replace the document with a Check And Swap operation (for optimistic concurrency)
     * <p>
     * Otherwise it {@link CouchbaseRepository#save(String, CouchbaseEntity)}
     *
     * @param id     the per-bucket unique document id
     * @param entity entity to convert and save
     * @return saved entity with CAS
     * @throws com.couchbase.client.java.error.CASMismatchException           if the cas of entity is different from existing one
     * @throws com.couchbase.client.java.error.DocumentAlreadyExistsException if the cas is not set and the document exists on couchbase
     */
    E checkAndSave(String id, E entity);

    /**
     * Removes a {@link CouchbaseEntity} from Couchbase Bucket identified by its id
     *
     * @param id the id of the document to remove
     */
    void delete(String id);

    /**
     * Retrieves a document from Couchbase {@link Bucket} by its ID.
     * <p>
     * - If the document exists, convert it to {@link CouchbaseEntity} with CAS set (Check And Swap for optimistic concurrency)
     * <br>
     * - Otherwise it return null
     * </p>
     * @param id the id of the document
     * @return the found and converted {@link CouchbaseEntity} with CAS set, or null if absent
     */
    E findOne(String id);

    /**
     * Save a json document buy its ID
     *
     * @param id          the per-bucket unique document id
     * @param jsonContent content of the json document
     */
    void save(String id, String jsonContent);

    /**
     * Inserts a {@link DesignDocument} into production
     *
     * @param name        name of the {@link DesignDocument} to insert
     * @param jsonContent the content of the {@link DesignDocument} to insert
     */
    void importDesignDoc(String name, String jsonContent);

    /**
     * Queries Couchbase {@link Bucket} with a {@link N1qlQuery}
     *
     * @param request {@link N1qlQuery} in String format
     */
    void query(String request);

    /**
     * @return name of the repository Couchbase {@link Bucket}
     */
    String getBucketName();
}
