package com.github.couchmove;

import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.query.QueryIndex;
import com.couchbase.client.java.manager.view.DesignDocument;
import com.couchbase.client.java.view.DesignDocumentNamespace;
import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.*;
import com.github.couchmove.repository.CouchbaseRepository;
import com.github.couchmove.repository.CouchbaseRepositoryImpl;
import com.github.couchmove.service.ChangeLogDBService;
import com.github.couchmove.utils.BaseIT;
import lombok.var;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.couchmove.pojo.Status.*;
import static com.github.couchmove.pojo.Type.*;
import static com.github.couchmove.service.ChangeLogDBService.PREFIX_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author ctayeb
 * Created on 05/06/2017
 */
public class CouchmoveIT extends BaseIT {

    public static final String DB_MIGRATION = "db/migration/";
    public static final String COUCHMOVE_SCOPE = "couchmove";
    public static final String COLLECTION = "changelog";
    public static final String TEST_SCOPE = "test";

    private static CouchbaseRepository<ChangeLog> changeLogRepository;

    private static ChangeLogDBService changeLogDBService;

    private static CouchbaseRepositoryImpl<User> userRepository;

    @BeforeEach
    public void init() {
        changeLogRepository = new CouchbaseRepositoryImpl<>(getCluster(), getBucket(), ChangeLog.class);
        changeLogDBService = new ChangeLogDBService(getBucket(), getCluster());
        userRepository = new CouchbaseRepositoryImpl<>(getCluster(), getBucket(), User.class);
    }

    @AfterEach
    public void clean() {
        CollectionManager collections = getBucket().collections();
        try {
            collections.dropScope(TEST_SCOPE);
            collections.dropScope(COUCHMOVE_SCOPE);
        } catch (ScopeNotFoundException e) {
            // Ignore if scopes doesn't exist
        }
    }

    @Test
    public void should_migrate_successfully() {
        // Given a Couchmove instance configured for success migration folder
        Couchmove couchmove = getCouchmove("success");

        // When we launch migration
        couchmove.migrate();

        // Then all changeLogs should be inserted in DB
        List<ChangeLog> changeLogs = Stream.of("0", "0.1", "1", "2")
                .map(version -> PREFIX_ID + version)
                .map(changeLogRepository::findOne)
                .collect(Collectors.toList());

        assertEquals(4, changeLogs.size());
        assertLike(changeLogs.get(0),
                "0", 1, "create index", N1QL, "V0__create_index.n1ql",
                "1a417b9f5787e52a46bc65bcd801e8f3f096e63ebcf4b0a17410b16458124af3",
                EXECUTED);
        assertLike(changeLogs.get(1),
                "0.1", 2, "insert users", DOCUMENTS, "V0.1__insert_users",
                "99a4aaf12e7505286afe2a5b074f7ebabd496f3ea8c4093116efd3d096c430a8",
                EXECUTED);
        assertLike(changeLogs.get(2),
                "1", 3, "user", DESIGN_DOC, "V1__user.json",
                "22df7f8496c21a3e1f3fbd241592628ad6a07797ea5d501df8ab6c65c94dbb79",
                EXECUTED);

        assertLike(changeLogs.get(3),
                "2", 4, "name", FTS, "V2__name.fts",
                "6ef9c3cc661804f7f0eb489e678971619a81b5457cff9355e28db9dbf835ea0a",
                EXECUTED);

        // And successfully executed

        // Users inserted
        assertEquals(new User("user", "titi", "01/09/1998"), userRepository.findOne("user::titi"));
        assertEquals(new User("user", "toto", "10/01/1991"), userRepository.findOne("user::toto"));

        // Index inserted
        Optional<QueryIndex> userIndexInfo = getCluster().queryIndexes().getAllIndexes(getBucket().name()).stream()
                .filter(i -> i.name().equals("user_index"))
                .findFirst();
        assertTrue(userIndexInfo.isPresent());
        assertEquals("`username`", userIndexInfo.get().indexKey().get(0));

        // Design Document inserted
        DesignDocument designDocument = getBucket().viewIndexes().getDesignDocument("user", DesignDocumentNamespace.PRODUCTION);
        assertNotNull(designDocument);

        // FTS index inserted
        assertThat(userRepository.isFtsIndexExists("name")).isTrue();
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
        getCouchmove("skip").migrate();

        // Then the old ChangeLog is marked as skipped
        assertLike(changeLogRepository.findOne(PREFIX_ID + "1.2"), "1.2", null, "type", DESIGN_DOC, "V1.2__type.json", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", SKIPPED);
    }

    @Test
    public void should_migration_fail_on_exception() {
        // Given a Couchmove instance configured for fail migration folder
        Couchmove couchmove = getCouchmove("fail");

        // When we launch migration, then an exception should be raised
        assertThrows(CouchmoveException.class, couchmove::migrate);

        // Then new ChangeLog is executed
        assertLike(changeLogRepository.findOne(PREFIX_ID + "1"), "1", 1, "insert users", N1QL, "V1__insert_users.n1ql", "a4b082eb19477034060ba02f60a7d40f39588e8d6fa6618b26b94cc6916d6cc3", EXECUTED);

        assertEquals(new User("admin", "Administrator", "01/09/1998"), userRepository.findOne("user::Administrator"));

        // And the ChangeLog marked as failed
        assertLike(changeLogRepository.findOne(PREFIX_ID + "2"), "2", null, "invalid request", N1QL, "V2__invalid_request.n1ql", "890c7bac55666a3073059c57f34e358f817e275eb68932e946ca35e9dcd428fe", FAILED);
    }

    @Test
    public void should_fixed_failed_migration_pass() {
        // Given a Couchmove instance configured for fail migration folder
        Couchmove couchmove = getCouchmove("fail");

        // When we launch migration, then an exception should be raised
        assertThrows(CouchmoveException.class, couchmove::migrate);

        // Given a Couchmove instance configured for fixed-fail migration folder
        couchmove = getCouchmove("fixed-fail");

        // When we relaunch migration
        couchmove.migrate();

        // Then it should be success
        assertLike(changeLogRepository.findOne(PREFIX_ID + "1"), "1", 1, "insert users", N1QL, "V1__insert_users.n1ql", "a4b082eb19477034060ba02f60a7d40f39588e8d6fa6618b26b94cc6916d6cc3", EXECUTED);

        assertLike(changeLogRepository.findOne(PREFIX_ID + "2"), "2", 2, "invalid request", N1QL, "V2__invalid_request.n1ql",
                "8fd2066ea5ad4e4151cc5b1262542455f41e7bcefe447cbcfbc004c6fe3bac12", EXECUTED);

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
        getCouchmove("update").migrate();

        // Then executed changeLog description updated
        assertLike(changeLogRepository.findOne(PREFIX_ID + "1"), "1", 1, "create index", N1QL, "V1__create_index.n1ql", "69eb9007c910c2b9cac46044a54de5e933b768ae874c6408356372576ab88dbd", EXECUTED);
    }

    @Test
    public void should_build_deferred_indexes() {
        // Given a Couchmove instance configured for success migration folder
        Couchmove couchmove = getCouchmove("multiple-deferred-indexes");

        // When we launch migration
        couchmove.migrate();

        // Then all changeLogs should be inserted in DB
        List<ChangeLog> changeLogs = Stream.of("0", "1")
                .map(version -> PREFIX_ID + version)
                .map(changeLogRepository::findOne)
                .collect(Collectors.toList());

        assertEquals(2, changeLogs.size());
        assertLike(changeLogs.get(0),
                "0", 1, "create deferred index", N1QL, "V0__create_deferred_index.n1ql",
                "8987fdc8782fe4f8321cfae8f388d9005ac6c2eca726105a2739170cc4870a66",
                EXECUTED);
        assertLike(changeLogs.get(1),
                "1", 2, "create second deferred index", N1QL, "V1__create_second_deferred_index.n1ql",
                "77492051f8633e40032881e474207d97d87c3eb1e239a832b1ad11b22c933fe6",
                EXECUTED);

        // Trigger deferred index build
        couchmove.buildN1qlDeferredIndexes();

        // Wait for indexes to be built
        couchmove.waitForN1qlIndexes(Duration.ofSeconds(5));

        // Index inserted
        Optional<QueryIndex> userIndexInfo = getCluster().queryIndexes().getAllIndexes(getBucket().name()).stream()
                .filter(i -> i.name().equals("buyer_index"))
                .findFirst();
        assertTrue(userIndexInfo.isPresent());
        assertEquals("`username`", userIndexInfo.get().indexKey().get(0));

        userIndexInfo = getCluster().queryIndexes().getAllIndexes(getBucket().name()).stream()
                .filter(i -> i.name().equals("merchant_index"))
                .findFirst();
        assertTrue(userIndexInfo.isPresent());
        assertEquals("`username`", userIndexInfo.get().indexKey().get(0));
    }

    @Test
    public void should_insert_collections() {
        // Given a Couchmove instance configured for collections migration folder
        Couchmove couchmove = getCouchmove("collections");

        // When we launch migration
        couchmove.migrate();

        // Then all changeLogs should be inserted in DB
        List<ChangeLog> changeLogs = Stream.of("0", "0.1")
                .map(version -> PREFIX_ID + version)
                .map(changeLogRepository::findOne)
                .collect(Collectors.toList());

        assertEquals(2, changeLogs.size());

        assertLike(changeLogs.get(0),
                "0", 1, "create scope and collection", N1QL, "V0__create_scope_and_collection.n1ql",
                "6c7949817c74cd62d480c4c5c0638f16dd111dca1b14ac560a6aee53d9617d0c",
                EXECUTED);

        assertLike(changeLogs.get(1),
                "0.1", 2, "insert users", DOCUMENTS, "V0.1__insert_users",
                "873831eed9a55a6e7d4445d39cbd2229c1bd41361d5ef9ab300bf56ad4f57940",
                EXECUTED);

        // Users inserted
        CouchbaseRepositoryImpl<User> userCollectionRepository = userRepository.withCollection(TEST_SCOPE, "user");
        assertEquals(new User("user", "titi", "01/09/1998"), userCollectionRepository.findOne("titi"));
        assertEquals(new User("user", "toto", "10/01/1991"), userCollectionRepository.findOne("toto"));
    }

    @Test
    public void should_insert_changelogs_into_collection() {
        // Given a new scope/collection
        CollectionManager collections = getBucket().collections();
        collections.createScope(COUCHMOVE_SCOPE);
        collections.createCollection(CollectionSpec.create(COLLECTION, COUCHMOVE_SCOPE));
        Collection collection = getBucket().scope(COUCHMOVE_SCOPE).collection(COLLECTION);

        // And a Couchmove instance configured with this collection
        var couchmove = new Couchmove(collection, getCluster(), DB_MIGRATION + "collections");

        // When we launch migration
        couchmove.migrate();

        // Then all changeLogs should be inserted in the same collection
        var collectionChangeLogRepository = changeLogRepository.withCollection(COUCHMOVE_SCOPE, COLLECTION);
        List<ChangeLog> changeLogs = Stream.of("0", "0.1")
                .map(version -> PREFIX_ID + version)
                .map(collectionChangeLogRepository::findOne)
                .collect(Collectors.toList());

        assertEquals(2, changeLogs.size());

        // Users inserted in the right collection
        CouchbaseRepositoryImpl<User> userCollectionRepository = userRepository.withCollection(TEST_SCOPE, "user");
        assertEquals(new User("user", "titi", "01/09/1998"), userCollectionRepository.findOne("titi"));
        assertEquals(new User("user", "toto", "10/01/1991"), userCollectionRepository.findOne("toto"));
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

    @NotNull
    private Couchmove getCouchmove(String path) {
        return new Couchmove(getBucket(), getCluster(), DB_MIGRATION + path);
    }
}
