package com.github.couchmove.utils;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Path;

import static com.github.couchmove.service.ChangeLogFileService.JSON;
import static com.github.couchmove.service.ChangeLogFileService.N1QL;

/**
 * Created by tayebchlyah on 02/06/2017.
 */
@RunWith(DataProviderRunner.class)
public class FileUtilsTest {

    public static final String DB_MIGRATION_PATH = "db/migration/";

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
                {DB_MIGRATION_PATH + "V1__create_index.n1ql", "bf0dae5d8fb638627eeabfb4b649d6100a8960da18859e12874e61063fbb16be"},
                {DB_MIGRATION_PATH + "V2__user.json", "22df7f8496c21a3e1f3fbd241592628ad6a07797ea5d501df8ab6c65c94dbb79"}
        };
    }

    @Test
    @UseDataProvider("fileProvider")
    public void should_calculate_checksum_of_file_or_folder(String path, String expectedChecksum) throws Exception {
        Assert.assertEquals(path, expectedChecksum, FileUtils.calculateChecksum(FileUtils.getPathFromResource(path).toFile(), JSON, N1QL));
    }
}