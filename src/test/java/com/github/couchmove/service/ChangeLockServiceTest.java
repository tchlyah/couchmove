package com.github.couchmove.service;

import com.github.couchmove.container.AbstractCouchbaseTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by tayebchlyah on 29/05/2017.
 */
public class ChangeLockServiceTest extends AbstractCouchbaseTest {

    @Test
    public void should_acquire_and_release_lock() {
        // Given a changeLockService
        ChangeLockService changeLockService = new ChangeLockService(getBucket());

        // When we tries to acquire lock
        Assert.assertTrue(changeLockService.acquireLock());

        // Then we should get it
        Assert.assertTrue(changeLockService.isLockAcquired());

        // When we release the lock
        changeLockService.releaseLock();

        // The it should be released
        Assert.assertFalse(changeLockService.isLockAcquired());
    }

    @Test
    public void should_not_acquire_lock_when_already_acquired() {
        // Given a first changeLockService that acquires lock
        ChangeLockService changeLockService1 = new ChangeLockService(getBucket());
        changeLockService1.acquireLock();

        // When an other changeLockService tries to get the lock
        ChangeLockService changeLockService2 = new ChangeLockService(getBucket());

        // Then it will fails
        Assert.assertFalse(changeLockService2.acquireLock());
        Assert.assertFalse(changeLockService2.isLockAcquired());

        // And the first service should keep the lock
        Assert.assertTrue(changeLockService1.isLockAcquired());
    }
}