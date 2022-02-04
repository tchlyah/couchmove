package com.github.couchmove.utils;

import com.github.couchmove.pojo.Document;
import com.google.common.io.Files;
import lombok.var;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static com.github.couchmove.pojo.Type.DESIGN_DOC;
import static com.github.couchmove.pojo.Type.N1QL;
import static com.github.couchmove.utils.TestUtils.getRandomString;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author ctayeb
 * Created on 02/06/2017
 */
public class FileUtilsTest {

    public static final String DB_MIGRATION_PATH = "db/migration/";
    public static final String SUCCESS_PATH = DB_MIGRATION_PATH + "success/";
    public static final String COLLECTION_PATH = DB_MIGRATION_PATH + "collections/";

    @Test
    public void should_get_file_path_from_resource() throws Exception {
        Path path = FileUtils.getPathFromResource(SUCCESS_PATH + "V1__user.json");
        assertThat(path).isNotNull();
        File file = path.toFile();
        assertThat(file.exists()).isTrue();
        assertThat(file.isFile()).isTrue();
    }

    @Test
    public void should_get_folder_path_from_resource() throws Exception {
        Path path = FileUtils.getPathFromResource(DB_MIGRATION_PATH);
        assertThat(path).isNotNull();
        File file = path.toFile();
        assertThat(file.exists()).isTrue();
        assertThat(file.isDirectory()).isTrue();
    }

    private static Stream<Arguments> fileSource() {
        return Stream.of(
                Arguments.of(SUCCESS_PATH + "V0.1__insert_users", "99a4aaf12e7505286afe2a5b074f7ebabd496f3ea8c4093116efd3d096c430a8"),
                Arguments.of(SUCCESS_PATH + "V0__create_index.n1ql", "1a417b9f5787e52a46bc65bcd801e8f3f096e63ebcf4b0a17410b16458124af3"),
                Arguments.of(SUCCESS_PATH + "V1__user.json", "22df7f8496c21a3e1f3fbd241592628ad6a07797ea5d501df8ab6c65c94dbb79"),
                Arguments.of(COLLECTION_PATH + "V0.1__insert_users", "873831eed9a55a6e7d4445d39cbd2229c1bd41361d5ef9ab300bf56ad4f57940")
        );
    }

    @ParameterizedTest
    @MethodSource("fileSource")
    public void should_calculate_checksum_of_file_or_folder(String path, String expectedChecksum) throws Exception {
        assertThat(FileUtils.calculateChecksum(FileUtils.getPathFromResource(path), DESIGN_DOC.getExtension(), N1QL.getExtension())).as(path).isEqualTo(expectedChecksum);
    }

    @Test
    public void should_read_files_failed_if_not_exists() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> FileUtils.readFilesInDirectory(new File(TestUtils.getRandomString()).toPath()));
    }

    @Test
    public void should_read_files_failed_if_not_directory() throws Exception {
        File temp = File.createTempFile(getRandomString(), "");
        temp.deleteOnExit();
        assertThrows(IllegalArgumentException.class, () -> FileUtils.readFilesInDirectory(temp.toPath()));
    }

    @Test
    public void should_read_files_in_directory() throws IOException {
        // Given a temp directory that contains
        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        // json file
        File file1 = File.createTempFile("file1", ".json", tempDir);
        String content1 = "content1";
        Files.write(content1.getBytes(), file1);
        // n1ql file
        File file2 = File.createTempFile("file2", ".N1QL", tempDir);
        String content2 = "content2";
        Files.write(content2.getBytes(), file2);
        // txt file
        Files.write(getRandomString().getBytes(), File.createTempFile(getRandomString(), ".txt", tempDir));
        // json file with scope/collection directories
        var scope = "scope";
        var collection = "collection";
        var collectionDir = new File(tempDir, format("%s/%s", scope, collection));
        collectionDir.mkdirs();
        File file3 = File.createTempFile("file3", ".json", collectionDir);
        String content3 = "content3";
        Files.write(content3.getBytes(), file3);

        // When we read files in this directory with extension filter
        var results = FileUtils.readFilesInDirectory(tempDir.toPath(), "json", "n1ql");

        // Then we should have file content matching this extension
        assertThat(results).hasSize(3);
        assertThat(results).containsExactlyInAnyOrder(
                new Document(null, null, file1.getName(), content1),
                new Document(null, null, file2.getName(), content2),
                new Document(scope, collection, file3.getName(), content3)
        );
    }
}
