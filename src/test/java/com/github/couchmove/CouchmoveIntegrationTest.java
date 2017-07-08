package com.github.couchmove;

import com.couchbase.client.java.query.util.IndexInfo;
import com.couchbase.client.java.view.DesignDocument;
import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.pojo.Status;
import com.github.couchmove.pojo.Type;
import com.github.couchmove.pojo.User;
import com.github.couchmove.repository.CouchbaseRepository;
import com.github.couchmove.repository.CouchbaseRepositoryImpl;
import com.github.couchmove.service.ChangeLogDBService;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.couchbase.AbstractCouchbaseTest;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.couchmove.pojo.Status.*;
import static com.github.couchmove.pojo.Type.*;
import static com.github.couchmove.service.ChangeLogDBService.PREFIX_ID;
import static com.github.couchmove.utils.TestUtils.assertThrows;
import static org.junit.Assert.*;

/**
 * @author ctayeb
 *         Created on 05/06/2017
 */
public class CouchmoveIntegrationTest extends AbstractCouchbaseTest {

    public static final String DB_MIGRATION = "db/migration/";

    private static CouchbaseRepository<ChangeLog> changeLogRepository;

    private static ChangeLogDBService changeLogDBService;

    private static CouchbaseRepositoryImpl<User> userRepository;

    @BeforeClass
    public static void init() {
        changeLogRepository = new CouchbaseRepositoryImpl<>(getBucket(), ChangeLog.class);
        changeLogDBService = new ChangeLogDBService(getBucket());
        userRepository = new CouchbaseRepositoryImpl<>(getBucket(), User.class);
    }

    @Test
    public void should_migrate_successfully() {
        // Given a Couchmove instance configured for success migration folder
        Couchmove couchmove = new Couchmove(getBucket(), DB_MIGRATION + "success");

        // When we launch migration
        couchmove.migrate();

        // Then all changeLogs should be inserted in DB
        List<ChangeLog> changeLogs = Stream.of("1", "1.1", "2")
                .map(version -> PREFIX_ID + version)
                .map(changeLogRepository::findOne)
                .collect(Collectors.toList());

        assertEquals(3, changeLogs.size());
        assertLike(changeLogs.get(0),
                "1", 1, "create index", N1QL, "V1__create_index.n1ql",
                "1a417b9f5787e52a46bc65bcd801e8f3f096e63ebcf4b0a17410b16458124af3",
                EXECUTED);
        assertLike(changeLogs.get(1),
                "1.1", 2, "insert users", DOCUMENTS, "V1.1__insert_users",
                "99a4aaf12e7505286afe2a5b074f7ebabd496f3ea8c4093116efd3d096c430a8",
                EXECUTED);
        assertLike(changeLogs.get(2),
                "2", 3, "user", Type.DESIGN_DOC, "V2__user.json",
                "22df7f8496c21a3e1f3fbd241592628ad6a07797ea5d501df8ab6c65c94dbb79",
                EXECUTED);

        // And successfully executed

        // Users inserted
        assertEquals(new User("user", "titi", "01/09/1998"), userRepository.findOne("user::titi"));
        assertEquals(new User("user", "toto", "10/01/1991"), userRepository.findOne("user::toto"));

        // Index inserted
        Optional<IndexInfo> userIndexInfo = getBucket().bucketManager().listN1qlIndexes().stream()
                .filter(i -> i.name().equals("user_index"))
                .findFirst();
        assertTrue(userIndexInfo.isPresent());
        assertEquals("`username`", userIndexInfo.get().indexKey().get(0));

        // Design Document inserted
        DesignDocument designDocument = getBucket().bucketManager().getDesignDocument("user");
        assertNotNull(designDocument);
    }

    @Test
    public void should_skip_old_migration_version() {
        // Given an executed changeLog
        changeLogDBService.save(ChangeLog.builder()
                .version("2")
                .order(3)
                .type(N1QL)
                .description("create index")
                .script("V2__create_index.n1ql")
                .checksum("69eb9007c910c2b9cac46044a54de5e933b768ae874c6408356372576ab88dbd")
                .runner("toto")
                .timestamp(new Date())
                .duration(400L)
                .status(EXECUTED)
                .build());

        // When we execute migration in skip migration folder
        new Couchmove(getBucket(), DB_MIGRATION + "skip").migrate();

        // Then the old ChangeLog is marked as skipped
        assertLike(changeLogRepository.findOne(PREFIX_ID + "1.2"), "1.2", null, "type", DESIGN_DOC, "V1.2__type.json", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", SKIPPED);
    }

    @Test
    public void should_migration_fail_on_exception() {
        // Given a Couchmove instance configured for fail migration folder
        Couchmove couchmove = new Couchmove(getBucket(), DB_MIGRATION + "fail");

        // When we launch migration, then an exception should be raised
        assertThrows(couchmove::migrate, CouchmoveException.class);

        // Then new ChangeLog is executed
        assertLike(changeLogRepository.findOne(PREFIX_ID + "1"), "1", 1, "insert users", N1QL, "V1__insert_users.n1ql", "efcc80f763e48e2a1d5b6689351ad1b4d678c70bebc0c0975a2d19f94e938f18", EXECUTED);

        assertEquals(new User("admin", "Administrator", "01/09/1998"), userRepository.findOne("user::Administrator"));

        // And the ChangeLog marked as failed
        assertLike(changeLogRepository.findOne(PREFIX_ID + "2"), "2", null, "invalid request", N1QL, "V2__invalid_request.n1ql", "890c7bac55666a3073059c57f34e358f817e275eb68932e946ca35e9dcd428fe", FAILED);
    }

    @Test
    public void should_fixed_failed_migration_pass() {
        // Given a Couchmove instance configured for fail migration folder
        Couchmove couchmove = new Couchmove(getBucket(), DB_MIGRATION + "fail");

        // When we launch migration, then an exception should be raised
        assertThrows(couchmove::migrate, CouchmoveException.class);

        // Given a Couchmove instance configured for fixed-fail migration folder
        couchmove = new Couchmove(getBucket(), DB_MIGRATION + "fixed-fail");

        // When we relaunch migration
        couchmove.migrate();

        // Then it should be success
        assertLike(changeLogRepository.findOne(PREFIX_ID + "1"), "1", 1, "insert users", N1QL, "V1__insert_users.n1ql", "efcc80f763e48e2a1d5b6689351ad1b4d678c70bebc0c0975a2d19f94e938f18", EXECUTED);

        assertLike(changeLogRepository.findOne(PREFIX_ID + "2"), "2", 2, "invalid request", N1QL, "V2__invalid_request.n1ql",
                "778c69b64c030eec8b33eb6ebf955954a3dfa20cab489021a2b71d445d5c3e54", EXECUTED);

        assertEquals(new User("user", "toto", "06/03/1997"), userRepository.findOne("user::toto"));
    }

    @Test
    public void should_update_changeLog() {
        // Given an executed changeLog
        changeLogDBService.save(ChangeLog.builder()
                .version("1")
                .order(1)
                .type(N1QL)
                .description("insert users")
                .script("V2__insert_users.n1ql")
                .checksum("69eb9007c910c2b9cac46044a54de5e933b768ae874c6408356372576ab88dbd")
                .runner("toto")
                .timestamp(new Date())
                .duration(400L)
                .status(EXECUTED)
                .build());

        // When we execute migration in update migration folder
        new Couchmove(getBucket(), DB_MIGRATION + "update").migrate();

        // Then executed changeLog description updated
        assertLike(changeLogRepository.findOne(PREFIX_ID + "1"), "1", 1, "create index", N1QL, "V1__create_index.n1ql", "69eb9007c910c2b9cac46044a54de5e933b768ae874c6408356372576ab88dbd", EXECUTED);
    }

    private static void assertLike(ChangeLog changeLog, String version, Integer order, String description, Type type, String script, String checksum, Status status) {
        assertNotNull("ChangeLog", changeLog);
        assertEquals("version", version, changeLog.getVersion());
        assertEquals("order", order, changeLog.getOrder());
        assertEquals("description", description, changeLog.getDescription());
        assertEquals("type", type, changeLog.getType());
        assertEquals("script", script, changeLog.getScript());
        assertEquals(checksum, changeLog.getChecksum());
        assertEquals(status, changeLog.getStatus());
        if (changeLog.getStatus() != SKIPPED) {
            assertNotNull("runner", changeLog.getRunner());
            assertNotNull("timestamp", changeLog.getTimestamp());
            assertNotNull("duration", changeLog.getDuration());
        }
    }

}
