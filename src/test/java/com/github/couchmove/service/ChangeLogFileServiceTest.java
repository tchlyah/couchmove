package com.github.couchmove.service;

import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.pojo.Type;
import com.google.common.io.Files;
import lombok.var;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.couchmove.utils.TestUtils.getRandomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author ctayeb
 * Created on 01/06/2017
 */
public class ChangeLogFileServiceTest {

    @Test
    public void should_fail_if_path_does_not_exists() {
        String folderPath;
        //noinspection StatementWithEmptyBody
        while (new File(folderPath = getRandomString()).exists()) ;
        String finalFolderPath = folderPath;
        assertThrows(CouchmoveException.class, () -> ChangeLogFileService.initializeFolder(finalFolderPath));
    }

    @Test
    public void should_fail_if_path_is_not_directory() throws Exception {
        File tempFile = File.createTempFile(getRandomString(), "");
        tempFile.deleteOnExit();
        assertThrows(CouchmoveException.class, () -> ChangeLogFileService.initializeFolder(tempFile.getPath()));
    }

    @Test
    public void should_get_right_type_from_file() {
        // For folder
        assertThat(ChangeLogFileService.getChangeLogType(FileUtils.getTempDirectory().toPath())).isEqualTo(Type.DOCUMENTS);
        // For JSON file
        assertThat(ChangeLogFileService.getChangeLogType(Paths.get("toto.json"))).isEqualTo(Type.DESIGN_DOC);
        assertThat(ChangeLogFileService.getChangeLogType(Paths.get("toto.JSON"))).isEqualTo(Type.DESIGN_DOC);
        // For N1QL files
        assertThat(ChangeLogFileService.getChangeLogType(Paths.get("toto.n1ql"))).isEqualTo(Type.N1QL);
        assertThat(ChangeLogFileService.getChangeLogType(Paths.get("toto.N1QL"))).isEqualTo(Type.N1QL);
        // For FTS files
        assertThat(ChangeLogFileService.getChangeLogType(Paths.get("toto.fts"))).isEqualTo(Type.FTS);
        assertThat(ChangeLogFileService.getChangeLogType(Paths.get("toto.FtS"))).isEqualTo(Type.FTS);
        // For Eventing files
        Assert.assertEquals(Type.EVENTING, ChangeLogFileService.getChangeLogType(Paths.get("toto.eventing")));
        Assert.assertEquals(Type.EVENTING, ChangeLogFileService.getChangeLogType(Paths.get("toto.Eventing")));
    }

    @Test
    public void should_throw_exception_when_unknown_file_type() {
        assertThrows(CouchmoveException.class, () -> ChangeLogFileService.getChangeLogType(Paths.get("titi.toto")));
    }

    @Test
    public void should_fetch_changeLogs() throws IOException {
        List<ChangeLog> changeLogs = Stream.of(
                        ChangeLog.builder()
                                .type(Type.N1QL)
                                .script("V0__create_index.n1ql")
                                .version("0")
                                .description("create index")
                                .checksum("1a417b9f5787e52a46bc65bcd801e8f3f096e63ebcf4b0a17410b16458124af3")
                                .build(),
                        ChangeLog.builder()
                                .type(Type.DOCUMENTS)
                                .script("V0.1__insert_users")
                                .version("0.1")
                                .description("insert users")
                                .checksum("99a4aaf12e7505286afe2a5b074f7ebabd496f3ea8c4093116efd3d096c430a8")
                                .build(),
                        ChangeLog.builder()
                                .type(Type.DESIGN_DOC)
                                .script("V1__user.json")
                                .version("1")
                                .description("user")
                                .checksum("22df7f8496c21a3e1f3fbd241592628ad6a07797ea5d501df8ab6c65c94dbb79")
                                .build(),
                        ChangeLog.builder()
                                .type(Type.FTS)
                                .script("V2__name.fts")
                                .version("2")
                                .description("name")
                                .checksum("6ef9c3cc661804f7f0eb489e678971619a81b5457cff9355e28db9dbf835ea0a")
                                .build(),
                ChangeLog.builder()
                        .type(Type.EVENTING)
                        .script("V3__test.eventing")
                        .version("3")
                        .description("test")
                        .checksum("d088945774720c9fd625394dca7528dd88eba8a12378ad312117eca3c3f8f645")
                        .build())
                .collect(Collectors.toList());
        assertThat(new ChangeLogFileService("db/migration/success").fetch()).isEqualTo(changeLogs);
    }

    @Test
    public void should_fetch_files_with_same_version() throws IOException {
        // Given a temp directory that contains
        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();

        // Create files with same version
        File file1 = File.createTempFile("V1__description_1", ".json", tempDir);
        String content1 = "content1";
        Files.write(content1.getBytes(), file1);
        // n1ql file
        File file2 = File.createTempFile("V1__description_2", ".n1ql", tempDir);
        String content2 = "content2";
        Files.write(content2.getBytes(), file2);

        // When we fetch changelogs in this directory
        var results = new ChangeLogFileService(tempDir.toPath().toString()).fetch();

        // Then we should get have this changelogs in the same order
        assertThat(results).hasSize(2);
        assertThat(results)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("checksum", "description")
                .containsExactlyInAnyOrder(
                        ChangeLog.builder()
                                .type(Type.DESIGN_DOC)
                                .script(file1.getName())
                                .version("1")
                                .build(),
                        ChangeLog.builder()
                                .type(Type.N1QL)
                                .script(file2.getName())
                                .version("1")
                                .build()
                );
    }

}
