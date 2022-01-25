package com.github.couchmove.repository;

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.query.QueryIndex;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.manager.view.DesignDocument;
import com.couchbase.client.java.view.DesignDocumentNamespace;
import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.pojo.Type;
import com.github.couchmove.utils.*;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.couchmove.CouchmoveIT.DB_MIGRATION;
import static com.github.couchmove.utils.TestUtils.getRandomString;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * @author ctayeb
 * Created on 28/05/2017
 */
public class CouchbaseRepositoryIT extends BaseIT {

    public static final String INDEX_NAME = "name";

    public static final String TEST = "test";

    @BeforeAll
    static void init() {
        getBucket().collections().createCollection(CollectionSpec.create("collection"));
    }

    static Stream<Arguments> repositoryParams() {
        return Stream.of(
                arguments("Bucket", new CouchbaseRepositoryImpl<>(getCluster(), getBucket(), ChangeLog.class)),
                arguments("Collection", new CouchbaseRepositoryImpl<>(getCluster(), getBucket().collection("collection"), ChangeLog.class))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("repositoryParams")
    public void should_save_and_get_entity(String description, CouchbaseRepository<ChangeLog> repository) {
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("repositoryParams")
    public void should_delete_entity(String description, CouchbaseRepository<ChangeLog> repository) {
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("repositoryParams")
    public void should_not_replace_entity_without_cas(String description, CouchbaseRepository<ChangeLog> repository) {
        // Given a changeLog saved on couchbase
        ChangeLog changeLog = TestUtils.getRandomChangeLog();
        String id = getRandomString();
        repository.save(id, changeLog);

        // When we tries to insert it without cas
        changeLog.setCas(null);

        // Then we should have exception upon saving with cas operation
        assertThrows(DocumentExistsException.class, () -> repository.checkAndSave(id, changeLog));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("repositoryParams")
    public void should_not_insert_entity_with_different_cas(String description, CouchbaseRepository<ChangeLog> repository) {
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
        assertThrows(CasMismatchException.class, () -> repository.checkAndSave(id, savedChangeLog));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("repositoryParams")
    public void should_import_design_doc(String description, CouchbaseRepository<ChangeLog> repository) {
        // Given a Design Doc
        String name = "user";
        String design_doc = "{\n" +
                "  \"views\": {\n" +
                "    \"findUser\": {\n" +
                "      \"map\": \"function (doc, meta) {\\n  if (doc.type == \\\"user\\\") {\\n    emit(doc.username, null);\\n  } \\n}\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        // When we import it
        repository.importDesignDoc(name, design_doc);

        // Then it should be saved
        DesignDocument designDocument = getBucket().viewIndexes().getDesignDocument(name, DesignDocumentNamespace.PRODUCTION);
        Assert.assertNotNull(designDocument);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("repositoryParams")
    public void should_import_fts_index(String description, CouchbaseRepository<ChangeLog> repository) throws IOException {
        // Given a fts index json definition file
        String ftsIndex = IOUtils.toString(FileUtils.getPathFromResource(DB_MIGRATION + "test.fts").toUri(), Charset.defaultCharset());

        // When we import it
        repository.importFtsIndex(TEST, ftsIndex);

        // Then it should be created
        assertThat(repository.isFtsIndexExists(TEST)).isTrue();

        // Get fts file contents
        Map<String, Object> ftsIndexMap = (Map<String, Object>) CouchbaseRepositoryImpl.getJsonMapper().readValue(ftsIndex, Map.class);

        // Ensure params is created as specified
        SearchIndex searchIndex = ((CouchbaseRepositoryImpl<?>) repository).getFtsIndex(TEST).get();
        assertThat(searchIndex.params()).isEqualTo(ftsIndexMap.get("params"));

        // Clean
        getCluster().searchIndexes().dropIndex(TEST);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("repositoryParams")
    public void should_check_fts_index_not_exists(String description, CouchbaseRepository<ChangeLog> repository) {
        assertThat(repository.isFtsIndexExists("toto")).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("repositoryParams")
    public void should_inject_bucket_name(String description, CouchbaseRepository<ChangeLog> repository) {
        String format = "SELECT * FROM `%s`";
        String statement = format(format, "${bucket}");
        Assert.assertEquals(format(format, getBucket().name()), ((CouchbaseRepositoryImpl) repository).injectParameters(statement));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("repositoryParams")
    public void should_execute_n1ql(String description, CouchbaseRepository<ChangeLog> repository) {
        // Given a primary index request
        String request = format("CREATE INDEX `%s` ON `${bucket}`(`%s`)", INDEX_NAME, INDEX_NAME);

        // When we execute the query
        repository.query(request);

        // Then the index should be Created
        List<QueryIndex> indexInfos = getCluster().queryIndexes().getAllIndexes(getBucket().name()).stream()
                .filter(indexInfo -> indexInfo.name().equals(INDEX_NAME))
                .collect(Collectors.toList());
        Assert.assertEquals(1, indexInfos.size());
        QueryIndex indexInfo = indexInfos.get(0);
        Assert.assertEquals(INDEX_NAME, indexInfo.name());
        Assert.assertEquals(format("`%s`", INDEX_NAME), indexInfo.indexKey().get(0));

        // Clean
        getCluster().queryIndexes().dropIndex(getBucket().name(), INDEX_NAME);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("repositoryParams")
    public void should_execute_n1ql_parse_fail(String description, CouchbaseRepository<ChangeLog> repository) {
        // Given an invalid request
        String request = format("CREATE INDEX `%s`", INDEX_NAME);

        // When we execute the query
        assertThrows(CouchmoveException.class, () -> repository.query(request));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("repositoryParams")
    public void should_execute_n1ql_fail(String description, CouchbaseRepository<ChangeLog> repository) {
        // Given an index on invalid bucket
        String request = format("CREATE INDEX `%s` on toto(%s)", INDEX_NAME, INDEX_NAME);

        // When we execute the query
        assertThrows(CouchmoveException.class, () -> repository.query(request));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("repositoryParams")
    public void should_save_json_document(String description, CouchbaseRepository<ChangeLog> repository) {
        // Given a json document
        String json = "{\n" +
                "  \"version\": \"1\",\n" +
                "  \"description\": \"insert users\",\n" +
                "  \"type\": \"N1QL\"\n" +
                "}";

        // When we save the document
        repository.save("change::1", json);

        // Then we should be bale to get it
        ChangeLog changeLog = repository.findOne("change::1");
        Assert.assertNotNull(changeLog);
        Assert.assertEquals("1", changeLog.getVersion());
        Assert.assertEquals("insert users", changeLog.getDescription());
        Assert.assertEquals(Type.N1QL, changeLog.getType());
    }

}
