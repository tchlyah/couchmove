package com.github.couchmove.utils;

import com.google.common.io.Files;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static com.github.couchmove.pojo.Type.DESIGN_DOC;
import static com.github.couchmove.pojo.Type.N1QL;
import static com.github.couchmove.utils.TestUtils.getRandomString;

/**
 * @author ctayeb
 * Created on 02/06/2017
 */
@RunWith(DataProviderRunner.class)
public class FileUtilsTest {

    public static final String DB_MIGRATION_PATH = "db/migration/success/";

    @Test
    public void should_get_file_path_from_resource() throws Exception {
        Path path = FileUtils.getPathFromResource(DB_MIGRATION_PATH + "V2__user.json");
        Assert.assertNotNull(path);
        File file = path.toFile();
        Assert.assertTrue(file.exists());
        Assert.assertTrue(file.isFile());
    }

    @Test
    public void should_get_folder_path_from_resource() throws Exception {
        Path path = FileUtils.getPathFromResource(DB_MIGRATION_PATH);
        Assert.assertNotNull(path);
        File file = path.toFile();
        Assert.assertTrue(file.exists());
        Assert.assertTrue(file.isDirectory());
    }

    @DataProvider
    public static Object[][] fileProvider() {
        return new Object[][]{
                {DB_MIGRATION_PATH + "V1.1__insert_users", "99a4aaf12e7505286afe2a5b074f7ebabd496f3ea8c4093116efd3d096c430a8"},
                {DB_MIGRATION_PATH + "V1__create_index.n1ql", "1a417b9f5787e52a46bc65bcd801e8f3f096e63ebcf4b0a17410b16458124af3"},
                {DB_MIGRATION_PATH + "V2__user.json", "22df7f8496c21a3e1f3fbd241592628ad6a07797ea5d501df8ab6c65c94dbb79"}
        };
    }

    @Test
    @UseDataProvider("fileProvider")
    public void should_calculate_checksum_of_file_or_folder(String path, String expectedChecksum) throws Exception {
        Assert.assertEquals(path, expectedChecksum, FileUtils.calculateChecksum(FileUtils.getPathFromResource(path), DESIGN_DOC.getExtension(), N1QL.getExtension()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_read_files_failed_if_not_exists() throws Exception {
        FileUtils.readFilesInDirectory(new File(TestUtils.getRandomString()).toPath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_read_files_failed_if_not_directory() throws Exception {
        File temp = File.createTempFile(getRandomString(), "");
        temp.deleteOnExit();
        FileUtils.readFilesInDirectory(temp.toPath());
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

        // When we read files in this directory with extension filter
        Map<String, String> result = FileUtils.readFilesInDirectory(tempDir.toPath(), "json", "n1ql");

        // Then we should have file content matching this extension
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(content1, result.get(file1.getName()));
        Assert.assertEquals(content2, result.get(file2.getName()));
    }
}