package com.github.couchmove.service;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.github.couchmove.pojo.ChangeLock;
import com.github.couchmove.repository.CouchbaseRepository;
import com.github.couchmove.repository.CouchbaseRepositoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.UUID;

/**
 * Created by tayebchlyah on 28/05/2017.
 */
public class ChangeLockService {

    private static Logger logger = LoggerFactory.getLogger(ChangeLockService.class);

    private static final String LOCK_ID = "DATABASE_CHANGELOG_LOCK";

    private final CouchbaseRepository<ChangeLock> repository;

    private String uuid;

    public ChangeLockService(Bucket bucket) {
        this.repository = new CouchbaseRepositoryImpl<>(bucket, ChangeLock.class);
    }

    public boolean acquireLock() {
        logger.info("Trying to acquire bucket '{}' lock...", repository.getBucketName());
        // Verify if there is any lock on database
        ChangeLock lock = repository.findOne(LOCK_ID);
        // If none, create one
        if (lock == null) {
            lock = new ChangeLock();
        } else if (lock.isLocked()) {
            logger.warn("The database is already locked by '{}'", lock.getRunner());
            return false;
        }
        // Create Lock information
        lock.setLocked(true);
        lock.setTimestamp(new Date());
        lock.setRunner(getUsername());
        lock.setUuid(uuid = UUID.randomUUID().toString());
        // Tries to save it with Optimistic locking
        try {
            repository.checkAndSave(LOCK_ID, lock);
        } catch (CASMismatchException | DocumentAlreadyExistsException e) {
            // In case of exception, this means an other process got the lock, logging its information
            lock = repository.findOne(LOCK_ID);
            logger.warn("The bucket '{}' is already locked by '{}'", repository.getBucketName(), lock.getRunner());
            return false;
        }
        logger.info("Lock acquired");
        return true;
    }

    public boolean isLockAcquired() {
        ChangeLock lock = repository.findOne(LOCK_ID);
        if (lock == null) {
            return false;
        }
        if (!lock.isLocked()) {
            return false;
        }
        if (lock.getUuid() == null || !lock.getUuid().equals(uuid)) {
            logger.warn("Lock is acquired by another process");
            return false;
        }
        return true;
    }

    public void releaseLock() {
        logger.info("Release lock");
        repository.delete(LOCK_ID);
    }

    //<editor-fold desc="Helpers">
    private static String getUsername() {
        String osName = System.getProperty("os.name").toLowerCase();
        String className = null;
        String methodName = "getUsername";

        if (osName.contains("windows")) {
            className = "com.sun.security.auth.module.NTSystem";
            methodName = "getName";
        } else if (osName.contains("linux") || osName.contains("mac")) {
            className = "com.sun.security.auth.module.UnixSystem";
        } else if (osName.contains("solaris") || osName.contains("sunos")) {
            className = "com.sun.security.auth.module.SolarisSystem";
        }

        if (className != null) {
            try {
                Class<?> c = Class.forName(className);
                Method method = c.getDeclaredMethod(methodName);
                Object o = c.newInstance();
                return method.invoke(o).toString();
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                logger.error("Unable to get actual user name", e);
            }
        }
        return "unknown";
    }
    //</editor-fold>
}
