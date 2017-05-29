package com.github.couchmove.repository;

import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.github.couchmove.container.AbstractCouchbaseTest;
import com.github.couchmove.pojo.ChangeLog;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;

/**
 * Created by tayebchlyah on 28/05/2017.
 */
public class CouchbaseRepositoryTest extends AbstractCouchbaseTest {

    private static CouchbaseRepository<ChangeLog> repository;

    private static CouchbaseRepository<ChangeLog> initializeRepository() {
        return new CouchbaseRepositoryImpl<>(AbstractCouchbaseTest.getBucket(), ChangeLog.class);
    }

    @BeforeClass
    public static void setUp() {
        repository = initializeRepository();
    }

    @After
    public void clear() {
        AbstractCouchbaseTest.flush();
    }

    @Test
    public void should_save_and_get_entity() {
        // Given a changeLog
        ChangeLog changeLog = getRandomChangeLog();

        // When we insert it with an id
        String id = getRandomString();
        repository.save(id, changeLog);

        // Then we should get it by this id
        ChangeLog result = repository.findOne(id);

        Assert.assertNotNull(result);
        Assert.assertEquals(changeLog, result);
    }

    @Test
    public void should_delete_entity() {
        // Given a changeLog saved on couchbase
        ChangeLog changeLog = getRandomChangeLog();

        String id = getRandomString();
        repository.save(id, changeLog);
        Assert.assertNotNull(repository.findOne(id));

        // When we delete it
        repository.delete(id);

        // Then we no longer should get it
        Assert.assertNull(repository.findOne(id));
    }

    @Test(expected = DocumentAlreadyExistsException.class)
    public void should_not_replace_entity_without_cas() {
        // Given a changeLog saved on couchbase
        ChangeLog changeLog = getRandomChangeLog();
        String id = getRandomString();
        repository.save(id, changeLog);

        // When we tries to insert it without cas
        changeLog.setCas(null);

        // Then we should have exception upon saving with cas operation
        repository.checkAndSave(id, changeLog);
    }

    @Test(expected = CASMismatchException.class)
    public void should_not_insert_entity_with_different_cas() {
        // Given a changeLog saved on couchbase
        ChangeLog changeLog = getRandomChangeLog();
        String id = getRandomString();
        repository.save(id, changeLog);

        // Then it should have a cas
        ChangeLog savedChangeLog = repository.findOne(id);
        Assert.assertNotNull(savedChangeLog.getCas());

        // When we change this cas
        savedChangeLog.setCas(new Random().nextLong());

        // Then we should have exception upon saving
        repository.checkAndSave(id, savedChangeLog);
    }

    //<editor-fold desc="Helpers">
    @NotNull
    private static String getRandomString() {
        return UUID.randomUUID().toString();
    }

    @NotNull
    private static ChangeLog getRandomChangeLog() {
        Random random = new Random();
        return ChangeLog.builder()
                .description(getRandomString())
                .duration(random.nextInt())
                .checksum(random.nextInt())
                .success(true)
                .build();
    }
    //</editor-fold>

}