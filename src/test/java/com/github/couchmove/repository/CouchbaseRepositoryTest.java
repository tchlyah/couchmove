package com.github.couchmove.repository;

import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.github.couchmove.container.AbstractCouchbaseTest;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.utils.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

import static com.github.couchmove.utils.TestUtils.getRandomString;

/**
 * Created by tayebchlyah on 28/05/2017.
 */
public class CouchbaseRepositoryTest extends AbstractCouchbaseTest {

    private static CouchbaseRepository<ChangeLog> repository;

    @BeforeClass
    public static void setUp() {
        repository = new CouchbaseRepositoryImpl<>(AbstractCouchbaseTest.getBucket(), ChangeLog.class);
    }

    @Test
    public void should_save_and_get_entity() {
        // Given a changeLog
        ChangeLog changeLog = TestUtils.getRandomChangeLog();

        // When we insert it with an id
        String id = getRandomString();
        ChangeLog savedChangeLog = repository.save(id, changeLog);

        // Then inserted one should have a cas
        Assert.assertNotNull(savedChangeLog.getCas());

        // And we should get it by this id
        ChangeLog result = repository.findOne(id);

        Assert.assertNotNull(result);
        Assert.assertEquals(changeLog, result);

        // And it should have the same cas
        Assert.assertNotNull(result.getCas());
        Assert.assertEquals(savedChangeLog.getCas(), result.getCas());
    }

    @Test
    public void should_delete_entity() {
        // Given a changeLog saved on couchbase
        ChangeLog changeLog = TestUtils.getRandomChangeLog();

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
        ChangeLog changeLog = TestUtils.getRandomChangeLog();
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
        ChangeLog changeLog = TestUtils.getRandomChangeLog();
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

}