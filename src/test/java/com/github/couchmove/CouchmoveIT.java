package com.github.couchmove;

import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.manager.collection.*;
import com.couchbase.client.java.manager.query.*;
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

import static com.couchbase.client.java.manager.query.DropQueryIndexOptions.dropQueryIndexOptions;
import static com.couchbase.client.java.manager.query.GetAllQueryIndexesOptions.getAllQueryIndexesOptions;
import static com.github.couchmove.pojo.Status.*;
import static com.github.couchmove.pojo.Type.*;
import static com.github.couchmove.repository.CouchbaseRepositoryImpl.DEFAULT;
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
    public static final String CHANGELOG_COLLECTION = "changelog";
    public static final String TEST_SCOPE = "test";
    public static final String COLLECTION_1 = "collection1";
    public static final String COLLECTION_2 = "collection2";
    public static final String PRIMARY_INDEX = "#primary";
    public static final String CHANGELOG_SCOPE = "couchmove";

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
        String bucket = getBucket().name();
        CollectionManager collections = getBucket().collections();
        List<ScopeSpec> scopes = collections.getAllScopes();

        // Drop indexes
        QueryIndexManager queryIndexManager = getCluster().queryIndexes();
        scopes.stream()
                .map(ScopeSpec::collections)
                .flatMap(java.util.Collection::stream)
                .map(collection -> queryIndexManager.getAllIndexes(bucket, getAllQueryIndexesOptions().scopeName(collection.scopeName()).collectionName(collection.name())))
                .flatMap(java.util.Collection::stream)
                .filter(index -> !index.name().equals(PRIMARY_INDEX))
                .forEach(index -> {
                    try {
                        queryIndexManager.dropIndex(bucket, index.name(),
                                index.collectionName().isPresent() && index.scopeName().isPresent() ?
                                        dropQueryIndexOptions().collectionName(index.collectionName().get()).scopeName(index.scopeName().get()) :
                                        dropQueryIndexOptions());
                    } catch (IndexNotFoundException e) {
                        // Ignore if index not found
                    }
                });

        // Drop scopes
        scopes.stream()
                .map(ScopeSpec::name)
                .filter(scope -> !DEFAULT.equals(scope))
                .forEach(collections::dropScope);
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
        assertThat(userRepository.findOne("user::titi")).isEqualTo(new User("user", "titi", "01/09/1998"));
        assertThat(userRepository.findOne("user::toto")).isEqualTo(new User("user", "toto", "10/01/1991"));

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

        assertThat(userRepository.findOne("user::Administrator")).isEqualTo(new User("admin", "Administrator", "01/09/1998"));

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

        assertThat(userRepository.findOne("user::toto")).isEqualTo(new User("user", "toto", "06/03/1997"));
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
    public void should_build_deferred_indexes() throws InterruptedException {
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

        // Index inserted in deferred state
        checkIndexStatus("buyer_index", DEFAULT, DEFAULT, "deferred");
        checkIndexStatus("merchant_index", DEFAULT, DEFAULT, "deferred");

        // Trigger deferred index build
        couchmove.buildN1qlDeferredIndexes();

        Thread.sleep(2000);

        // Index inserted in building state
        checkIndexStatus("buyer_index", DEFAULT, DEFAULT, "building");
        checkIndexStatus("merchant_index", DEFAULT, DEFAULT, "building");

        // Wait for indexes to be built
        couchmove.waitForN1qlIndexes(Duration.ofSeconds(30));

        // Index online
        checkIndexStatus("buyer_index", DEFAULT, DEFAULT, "online");
        checkIndexStatus("merchant_index", DEFAULT, DEFAULT, "online");
    }

    @Test
    public void should_build_scope_deferred_indexes() throws InterruptedException {
        should_create_scope_deferred_indexes();

        Couchmove couchmove = getCouchmove("collections-deferred-indexes");

        // Trigger deferred index build on scope
        couchmove.buildN1qlDeferredIndexes(TEST_SCOPE);

        Thread.sleep(2000);

        // Index building
        checkIndexStatus("collection1_index", TEST_SCOPE, COLLECTION_1, "building");
        checkIndexStatus("collection2_index", TEST_SCOPE, COLLECTION_2, "building");

        // Wait for indexes to be built
        couchmove.waitForN1qlIndexes(TEST_SCOPE, Duration.ofSeconds(30));

        // Index online
        checkIndexStatus("collection1_index", TEST_SCOPE, COLLECTION_1, "online");
        checkIndexStatus("collection2_index", TEST_SCOPE, COLLECTION_2, "online");
    }

    @Test
    public void should_build_collection_deferred_indexes() throws InterruptedException {
        should_create_scope_deferred_indexes();

        Couchmove couchmove = getCouchmove("collections-deferred-indexes");

        // Trigger deferred index build on scope
        couchmove.buildN1qlDeferredIndexes(TEST_SCOPE, COLLECTION_1);

        Thread.sleep(2000);

        // Index building
        checkIndexStatus("collection1_index", TEST_SCOPE, COLLECTION_1, "building");
        checkIndexStatus("collection2_index", TEST_SCOPE, COLLECTION_2, "deferred");

        // Wait for indexes to be built
        couchmove.waitForN1qlIndexes(TEST_SCOPE, COLLECTION_1, Duration.ofSeconds(30));

        // Index online
        checkIndexStatus("collection1_index", TEST_SCOPE, COLLECTION_1, "online");
        checkIndexStatus("collection2_index", TEST_SCOPE, COLLECTION_2, "deferred");
    }

    private void should_create_scope_deferred_indexes() throws InterruptedException {
        // Given a Couchmove instance configured for success migration folder
        Couchmove couchmove = getCouchmove("collections-deferred-indexes");

        // When we launch migration
        couchmove.migrate();

        // Then all changeLogs should be inserted in DB
        List<ChangeLog> changeLogs = Stream.of("0", "1")
                .map(version -> PREFIX_ID + version)
                .map(changeLogRepository::findOne)
                .collect(Collectors.toList());

        assertEquals(2, changeLogs.size());
        assertLike(changeLogs.get(0),
                "0", 1, "create scope and collections", N1QL, "V0__create_scope_and_collections.n1ql",
                "14f8b6c4d9bbe6e990601c99b12079196d7b774489462b6228217cca48e9ba89",
                EXECUTED);
        assertLike(changeLogs.get(1),
                "1", 2, "create deferred indexes", N1QL, "V1__create_deferred_indexes.n1ql",
                "7320691fae304772dcf4186c8f4bafa9005066bb5908d7a18083a1d6191cdac3",
                EXECUTED);

        // Index inserted in deferred state
        checkIndexStatus("collection1_index", TEST_SCOPE, COLLECTION_1, "deferred");
        checkIndexStatus("collection2_index", TEST_SCOPE, COLLECTION_2, "deferred");
    }

    private void checkIndexStatus(String name, String scope, String collection, String status) {
        GetAllQueryIndexesOptions options = getAllQueryIndexesOptions();
        if (!scope.equals(DEFAULT) || !collection.equals(DEFAULT)) {
            options = options.scopeName(scope).collectionName(collection);
        }
        Optional<QueryIndex> indexInfo = getCluster().queryIndexes().getAllIndexes(getBucket().name(), options).stream()
                .filter(i -> i.name().equals(name))
                .findFirst();
        assertThat(indexInfo)
                .isPresent()
                .get()
                .satisfies(index -> {
                    if (!scope.equals(DEFAULT) || !collection.equals(DEFAULT)) {
                        assertThat(index.scopeName()).isPresent().get().isEqualTo(scope);
                        assertThat(index.collectionName()).isPresent().get().isEqualTo(collection);
                    }
                    assertThat(index.state()).isEqualTo(status);
                });
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
        assertThat(userCollectionRepository.findOne("titi")).isEqualTo(new User("user", "titi", "01/09/1998"));
        assertThat(userCollectionRepository.findOne("toto")).isEqualTo(new User("user", "toto", "10/01/1991"));
    }

    @Test
    public void should_insert_changelogs_into_collection() {
        // Given a new scope/collection
        Collection collection = getBucket().scope(CHANGELOG_SCOPE).collection(CHANGELOG_COLLECTION);

        // And a Couchmove instance configured with this collection
        var couchmove = new Couchmove(collection, getCluster(), DB_MIGRATION + "collections");

        // Couchmove should create scope and collection
        assertThat(
                getBucket().collections().getAllScopes().stream()
                        .filter(scopeSpec -> scopeSpec.name().equals(CHANGELOG_SCOPE))
                        .map(ScopeSpec::collections)
                        .flatMap(java.util.Collection::stream)
                        .map(CollectionSpec::name)
                        .filter(CHANGELOG_COLLECTION::equals)
                        .findFirst())
                .isPresent();

        // When we launch migration
        couchmove.migrate();

        // Then all changeLogs should be inserted in the same collection
        var collectionChangeLogRepository = changeLogRepository.withCollection(CHANGELOG_SCOPE, CHANGELOG_COLLECTION);
        List<ChangeLog> changeLogs = Stream.of("0", "0.1")
                .map(version -> PREFIX_ID + version)
                .map(collectionChangeLogRepository::findOne)
                .collect(Collectors.toList());

        assertEquals(2, changeLogs.size());

        // Users inserted in the right collection
        CouchbaseRepositoryImpl<User> userCollectionRepository = userRepository.withCollection(TEST_SCOPE, "user");
        assertThat(userCollectionRepository.findOne("titi")).isEqualTo(new User("user", "titi", "01/09/1998"));
        assertThat(userCollectionRepository.findOne("toto")).isEqualTo(new User("user", "toto", "10/01/1991"));
    }

    private static void assertLike(ChangeLog changeLog, String version, Integer order, String description, Type type, String script, String checksum, Status status) {
        assertThat(changeLog).as("ChangeLog").isNotNull();
        assertThat(changeLog.getVersion()).as("version").isEqualTo(version);
        assertThat(changeLog.getOrder()).as("order").isEqualTo(order);
        assertThat(changeLog.getDescription()).as("description").isEqualTo(description);
        assertThat(changeLog.getType()).as("type").isEqualTo(type);
        assertThat(changeLog.getScript()).as("script").isEqualTo(script);
        assertThat(changeLog.getChecksum()).as("checksum").isEqualTo(checksum);
        assertThat(changeLog.getStatus()).as("status").isEqualTo(status);
        if (changeLog.getStatus() != SKIPPED) {
            assertThat(changeLog.getRunner()).as("runner").isNotNull();
            assertThat(changeLog.getTimestamp()).as("timestamp").isNotNull();
            assertThat(changeLog.getDuration()).as("duration").isNotNull();
        }
    }

    @NotNull
    private Couchmove getCouchmove(String path) {
        return new Couchmove(getBucket(), getCluster(), DB_MIGRATION + path);
    }
}
