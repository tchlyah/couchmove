package com.github.couchmove.service;

import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.utils.CouchbaseTest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author ctayeb
 * Created on 29/05/2017
 */
public class ChangeLockServiceTest extends CouchbaseTest {

    @Test
    public void should_acquire_and_release_lock() {
        // Given a changeLockService
        ChangeLockService changeLockService = newChangeLockService();

        // When we tries to acquire lock
        assertTrue(changeLockService.acquireLock());

        // Then we should get it
        assertTrue(changeLockService.isLockAcquired());

        // When we release the lock
        changeLockService.releaseLock();

        // The it should be released
        assertFalse(changeLockService.isLockAcquired());
    }

    @NotNull
    private ChangeLockService newChangeLockService() {
        return new ChangeLockService(getBucket(), TEST_BUCKET, DEFAULT_PASSWORD);
    }

    @Test
    public void should_not_acquire_lock_when_already_acquired() {
        // Given a first changeLockService that acquires lock
        ChangeLockService changeLockService1 = newChangeLockService();
        changeLockService1.acquireLock();

        // When an other changeLockService tries to get the lock
        ChangeLockService changeLockService2 = newChangeLockService();

        // Then it will fails
        assertFalse(changeLockService2.acquireLock());
        assertFalse(changeLockService2.isLockAcquired());

        // And the first service should keep the lock
        assertTrue(changeLockService1.isLockAcquired());
    }

    @Test
    public void should_not_release_lock_acquired_by_another_process() {
        // Given a process holding the lock
        ChangeLockService changeLockService1 = newChangeLockService();
        changeLockService1.acquireLock();
        assertTrue(changeLockService1.isLockAcquired());

        // When an other process tries to release the lock
        ChangeLockService changeLockService2 = newChangeLockService();

        // Then it should fails
        assertThrows(CouchmoveException.class, changeLockService2::releaseLock);

        // When an other process force release the lock
        changeLockService2.forceReleaseLock();

        // Then the first should loose the lock
        assertFalse(changeLockService1.isLockAcquired());
    }

}
