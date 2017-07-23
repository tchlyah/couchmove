package com.github.couchmove.pojo;

import com.couchbase.client.java.Bucket;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

/**
 * a {@link CouchbaseEntity} representing a pessimistic locking of a Couchbase {@link Bucket}
 *
 * @author ctayeb
 * Created on 27/05/2017
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class ChangeLock extends CouchbaseEntity {

    /**
     * Determines if the {@link Bucket} is locked
     */
    private boolean locked;

    /**
     * Unique ID identifying instance that acquires the lock
     */
    private String uuid;

    /**
     * The OS username of the process holding the lock
     */
    private String runner;

    /**
     * The date when the {@link Bucket} was locked
     */
    private Date timestamp;
}
