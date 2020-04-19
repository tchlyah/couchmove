package com.github.couchmove.repository;

import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.query.util.IndexInfo;
import com.couchbase.client.java.view.DesignDocument;
import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.pojo.Type;
import com.github.couchmove.utils.CouchbaseTest;
import com.github.couchmove.utils.FileUtils;
import com.github.couchmove.utils.TestUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static com.github.couchmove.CouchmoveIntegrationTest.DB_MIGRATION;
import static com.github.couchmove.utils.TestUtils.getRandomString;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author ctayeb
 * Created on 28/05/2017
 */
public class CouchbaseRepositoryTest extends CouchbaseTest {

    public static final String INDEX_NAME = "name";

    public static final String TEST = "test";

    private static CouchbaseRepository<ChangeLog> repository;

    @BeforeEach
    public void setUp() {
        repository = new CouchbaseRepositoryImpl<>(getBucket(), getCouchbaseContainer().getUsername(), getCouchbaseContainer().getPassword(), ChangeLog.class);
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

    @Test
    public void should_not_replace_entity_without_cas() {
        // Given a changeLog saved on couchbase
        ChangeLog changeLog = TestUtils.getRandomChangeLog();
        String id = getRandomString();
        repository.save(id, changeLog);

        // When we tries to insert it without cas
        changeLog.setCas(null);

        // Then we should have exception upon saving with cas operation
        assertThrows(DocumentAlreadyExistsException.class, () -> repository.checkAndSave(id, changeLog));
    }

    @Test
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
        assertThrows(CASMismatchException.class, () -> repository.checkAndSave(id, savedChangeLog));
    }

    @Test
    public void should_import_design_doc() {
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
        DesignDocument designDocument = getBucket().bucketManager().getDesignDocument(name);
        Assert.assertNotNull(designDocument);
    }

    @Test
    public void should_import_fts_index() throws IOException {
        // Given a fts index json definition file
        String ftsIndex = IOUtils.toString(FileUtils.getPathFromResource(DB_MIGRATION + "success/V2__name.fts").toUri(), Charset.defaultCharset());

        // When we import it
        repository.importFtsIndex(TEST, ftsIndex);

        // Then it should be created
        assertThat(repository.isFtsIndexExists(TEST)).isTrue();
    }

    @Test
    public void should_check_fts_index_not_exists() {
        assertThat(repository.isFtsIndexExists("toto")).isFalse();
    }

    @Test
    public void should_inject_bucket_name() {
        String format = "SELECT * FROM `%s`";
        String statement = format(format, "${bucket}");
        Assert.assertEquals(format(format, getBucket().name()), ((CouchbaseRepositoryImpl) repository).injectParameters(statement));
    }

    @Test
    public void should_execute_n1ql() {
        // Given a primary index request
        String request = format("CREATE INDEX `%s` ON `${bucket}`(`%s`)", INDEX_NAME, INDEX_NAME);

        // When we execute the query
        repository.query(request);

        // Then the index should be Created
        List<IndexInfo> indexInfos = getBucket().bucketManager().listN1qlIndexes().stream()
                .filter(indexInfo -> indexInfo.name().equals(INDEX_NAME))
                .collect(Collectors.toList());
        Assert.assertEquals(1, indexInfos.size());
        IndexInfo indexInfo = indexInfos.get(0);
        Assert.assertEquals(INDEX_NAME, indexInfo.name());
        Assert.assertEquals(format("`%s`", INDEX_NAME), indexInfo.indexKey().get(0));
    }

    @Test
    public void should_execute_n1ql_parse_fail() {
        // Given an invalid request
        String request = format("CREATE INDEX `%s`", INDEX_NAME);

        // When we execute the query
        assertThrows(CouchmoveException.class, () -> repository.query(request));
    }

    @Test
    public void should_execute_n1ql_fail() {
        // Given an index on invalid bucket
        String request = format("CREATE INDEX `%s` on toto(%s)", INDEX_NAME, INDEX_NAME);

        // When we execute the query
        assertThrows(CouchmoveException.class, () -> repository.query(request));
    }

    @Test
    public void should_save_json_document() {
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
