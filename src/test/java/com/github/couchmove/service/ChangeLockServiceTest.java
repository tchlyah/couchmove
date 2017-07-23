package com.github.couchmove.service;

import org.testcontainers.couchbase.AbstractCouchbaseTest;
import com.github.couchmove.exception.CouchmoveException;
import org.junit.Test;

import static com.github.couchmove.utils.TestUtils.assertThrows;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author ctayeb
 *         Created on 29/05/2017
 */
public class ChangeLockServiceTest extends AbstractCouchbaseTest {

    @Test
    public void should_acquire_and_release_lock() {
        // Given a changeLockService
        ChangeLockService changeLockService = new ChangeLockService(getBucket());

        // When we tries to acquire lock
        assertTrue(changeLockService.acquireLock());

        // Then we should get it
        assertTrue(changeLockService.isLockAcquired());

        // When we release the lock
        changeLockService.releaseLock();

        // The it should be released
        assertFalse(changeLockService.isLockAcquired());
    }

    @Test
    public void should_not_acquire_lock_when_already_acquired() {
        // Given a first changeLockService that acquires lock
        ChangeLockService changeLockService1 = new ChangeLockService(getBucket());
        changeLockService1.acquireLock();

        // When an other changeLockService tries to get the lock
        ChangeLockService changeLockService2 = new ChangeLockService(getBucket());

        // Then it will fails
        assertFalse(changeLockService2.acquireLock());
        assertFalse(changeLockService2.isLockAcquired());

        // And the first service should keep the lock
        assertTrue(changeLockService1.isLockAcquired());
    }

    @Test
    public void should_not_release_lock_acquired_by_another_process() {
        // Given a process holding the lock
        ChangeLockService changeLockService1 = new ChangeLockService(getBucket());
        changeLockService1.acquireLock();
        assertTrue(changeLockService1.isLockAcquired());

        // When an other process tries to release the lock
        ChangeLockService changeLockService2 = new ChangeLockService(getBucket());

        // Then it should fails
        assertThrows(changeLockService2::releaseLock, CouchmoveException.class);

        // When an other process force release the lock
        changeLockService2.forceReleaseLock();

        // Then the first should loose the lock
        assertFalse(changeLockService1.isLockAcquired());
    }

}