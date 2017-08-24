package com.github.couchmove.service;

import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.pojo.Type;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.couchmove.utils.TestUtils.getRandomString;

/**
 * @author ctayeb
 * Created on 01/06/2017
 */
public class ChangeLogFileServiceTest {

    @Test(expected = CouchmoveException.class)
    public void should_fail_if_path_does_not_exists() {
        String folderPath;
        //noinspection StatementWithEmptyBody
        while (new File(folderPath = getRandomString()).exists()) ;
        ChangeLogFileService.initializeFolder(folderPath);
    }

    @Test(expected = CouchmoveException.class)
    public void should_fail_if_path_is_not_directory() throws Exception {
        File tempFile = File.createTempFile(getRandomString(), "");
        tempFile.deleteOnExit();
        ChangeLogFileService.initializeFolder(tempFile.getPath());
    }

    @Test
    public void should_get_right_type_from_file() {
        // For folder
        Assert.assertEquals(Type.DOCUMENTS, ChangeLogFileService.getChangeLogType(FileUtils.getTempDirectory().toPath()));
        // For JSON file
        Assert.assertEquals(Type.DESIGN_DOC, ChangeLogFileService.getChangeLogType(Paths.get("toto.json")));
        Assert.assertEquals(Type.DESIGN_DOC, ChangeLogFileService.getChangeLogType(Paths.get("toto.JSON")));
        // For N1QL files
        Assert.assertEquals(Type.N1QL, ChangeLogFileService.getChangeLogType(Paths.get("toto.n1ql")));
        Assert.assertEquals(Type.N1QL, ChangeLogFileService.getChangeLogType(Paths.get("toto.N1QL")));
    }

    @Test(expected = CouchmoveException.class)
    public void should_throw_exception_when_unknown_file_type() {
        ChangeLogFileService.getChangeLogType(Paths.get("toto"));
    }

    @Test
    public void should_fetch_changeLogs() throws IOException {
        List<ChangeLog> changeLogs = Stream.of(
                ChangeLog.builder()
                        .type(Type.N1QL)
                        .script("V1__create_index.n1ql")
                        .version("1")
                        .description("create index")
                        .checksum("1a417b9f5787e52a46bc65bcd801e8f3f096e63ebcf4b0a17410b16458124af3")
                        .build(),
                ChangeLog.builder()
                        .type(Type.DOCUMENTS)
                        .script("V1.1__insert_users")
                        .version("1.1")
                        .description("insert users")
                        .checksum("99a4aaf12e7505286afe2a5b074f7ebabd496f3ea8c4093116efd3d096c430a8")
                        .build(),
                ChangeLog.builder()
                        .type(Type.DESIGN_DOC)
                        .script("V2__user.json")
                        .version("2")
                        .description("user")
                        .checksum("22df7f8496c21a3e1f3fbd241592628ad6a07797ea5d501df8ab6c65c94dbb79")
                        .build())
                .collect(Collectors.toList());
        Assert.assertEquals(changeLogs, new ChangeLogFileService("db/migration/success").fetch());
    }

}