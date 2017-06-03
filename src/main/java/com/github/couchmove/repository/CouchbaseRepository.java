package com.github.couchmove.repository;

import com.couchbase.client.java.Bucket;
import com.github.couchmove.pojo.CouchbaseEntity;

/**
 * Created by tayebchlyah on 27/05/2017.
 */
public interface CouchbaseRepository<E extends CouchbaseEntity> {

    E save(String id, E entity);

    E checkAndSave(String id, E entity);

    void delete(String id);

    E findOne(String id);

    String getBucketName();
}
