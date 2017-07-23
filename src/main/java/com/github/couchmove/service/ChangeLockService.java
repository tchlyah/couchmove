package com.github.couchmove.service;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.ChangeLock;
import com.github.couchmove.repository.CouchbaseRepository;
import com.github.couchmove.repository.CouchbaseRepositoryImpl;
import com.github.couchmove.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

/**
 * Service for acquiring a pessimistic lock of a Couchbase {@link Bucket}
 *
 * @author ctayeb
 *         Created on 27/05/2017
 */
public class ChangeLockService {

    private static Logger logger = LoggerFactory.getLogger(ChangeLockService.class);

    private static final String LOCK_ID = "DATABASE_CHANGELOG_LOCK";

    private final CouchbaseRepository<ChangeLock> repository;

    private String uuid;

    public ChangeLockService(Bucket bucket) {
        this.repository = new CouchbaseRepositoryImpl<>(bucket, ChangeLock.class);
    }

    /**
     * Tries to acquire a pessimistic lock of Couchbase {@link Bucket}
     *
     * @return true if lock successfully acquired, false otherwise
     */
    public boolean acquireLock() {
        logger.info("Trying to acquire bucket '{}' change log lock...", repository.getBucketName());
        // Verify if there is any lock on database
        ChangeLock lock = repository.findOne(LOCK_ID);
        // If none, create one
        if (lock == null) {
            lock = new ChangeLock();
        } else if (lock.isLocked()) {
            logger.warn("The bucket is already locked by '{}'", lock.getRunner());
            return false;
        }
        // Create Lock information
        lock.setLocked(true);
        lock.setTimestamp(new Date());
        lock.setRunner(Utils.getUsername());
        lock.setUuid(uuid = UUID.randomUUID().toString());
        // Tries to save it with Optimistic locking
        try {
            repository.checkAndSave(LOCK_ID, lock);
        } catch (CASMismatchException | DocumentAlreadyExistsException e) {
            // In case of exception, this means an other process got the lock, logging its information
            lock = repository.findOne(LOCK_ID);
            logger.warn("The bucket is already locked by '{}'", lock.getRunner());
            return false;
        }
        logger.info("Successfully acquired change log lock");
        return true;
    }

    /**
     * Check if the Couchbase {@link Bucket} is actually locked by this instance
     *
     * @return true if the current instance holds the lock, false otherwise.
     */
    public boolean isLockAcquired() {
        ChangeLock lock = repository.findOne(LOCK_ID);
        if (lock == null) {
            return false;
        }
        if (!lock.isLocked()) {
            return false;
        }
        if (lock.getUuid() == null || !lock.getUuid().equals(uuid)) {
            logger.warn("Change log lock is acquired by another process");
            return false;
        }
        return true;
    }

    /**
     * Releases the pessimistic lock of Couchbase {@link Bucket}
     */
    public void releaseLock() {
        if (isLockAcquired()) {
            forceReleaseLock();
        } else {
            throw new CouchmoveException("Unable to release lock acquired by an other process");
        }
    }

    /**
     * Force release pessimistic lock even if the current instance doesn't hold it
     */
    public void forceReleaseLock() {
        repository.delete(LOCK_ID);
        logger.info("Successfully released change log lock");
    }
}
