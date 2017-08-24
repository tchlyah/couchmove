package com.github.couchmove.utils;

import com.github.couchmove.exception.CouchmoveException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.github.couchmove.utils.FunctionUtils.unchecked;
import static org.apache.commons.io.IOUtils.toByteArray;

/**
 * @author ctayeb
 * Created on 02/06/2017
 */
public class FileUtils {

    /**
     * Returns Path of a resource in classpath no matter whether it is in a jar or in absolute or relative folder
     *
     * @param resource path
     * @return Path of a resource
     * @throws IOException if an I/O error occurs
     */
    public static Path getPathFromResource(String resource) throws IOException {
        File file = new File(resource);
        if (file.exists()) {
            return file.toPath();
        }
        URL resourceURL = Thread.currentThread().getContextClassLoader().getResource(resource);
        if (resourceURL == null) {
            resourceURL = FileUtils.class.getResource(resource);
        }
        if (resourceURL == null) {
            throw new FileNotFoundException(resource);
        }
        URI uri;
        try {
            uri = resourceURL.toURI();
        } catch (URISyntaxException e) {
            // Can not happen normally
            throw new RuntimeException(e);
        }
        if (uri.getScheme().equals("jar")) {
            FileSystem fileSystem;
            try {
                fileSystem = FileSystems.getFileSystem(uri);
            } catch (FileSystemNotFoundException e) {
                fileSystem = FileSystems.newFileSystem(uri, Collections.emptyMap());
            }
            Path path = fileSystem.getPath(resource);
            String fullUri = resourceURL.getFile().substring(resourceURL.getFile().indexOf("!")).replaceAll("!", "");
            return path.resolve(fullUri);
        } else {
            return Paths.get(uri);
        }
    }

    /**
     * If the file is a Directory, calculate the checksum of all files in this directory (one level)
     * Else, calculate the checksum of the file matching extensions
     *
     * @param filePath   file or folder
     * @param extensions of files to calculate checksum of
     * @return checksum
     */
    public static String calculateChecksum(@NotNull Path filePath, String... extensions) {
        if (filePath == null || !Files.exists(filePath)) {
            throw new CouchmoveException("File is null or doesn't exists");
        }
        if (Files.isDirectory(filePath)) {
            return directoryStream(filePath, extensions)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .map(FileUtils::calculateChecksum)
                    .reduce(String::concat)
                    .map(DigestUtils::sha256Hex)
                    .orElse(null);
        }
        try {
            return DigestUtils.sha256Hex(toByteArray(filePath.toUri()));
        } catch (IOException e) {
            throw new CouchmoveException("Unable to calculate file checksum '" + filePath.getFileName().toString() + "'");
        }
    }

    /**
     * Read files content from a (@link File}
     *
     * @param directoryPath The directory containing files to read
     * @param extensions    The extensions of the files to read
     * @return {@link Map} which keys represents the name (with extension), and values the content of read files
     */
    public static Map<String, String> readFilesInDirectory(Path directoryPath, String... extensions) {
        if (directoryPath == null || !Files.exists(directoryPath)) {
            throw new IllegalArgumentException("File is null or doesn't exists");
        }
        if (!Files.isDirectory(directoryPath)) {
            throw new IllegalArgumentException("'" + directoryPath + "' is not a directory");
        }
        return directoryStream(directoryPath, extensions)
                .collect(Collectors.toMap(path -> path.getFileName().toString(),
                        unchecked(path -> new String(IOUtils.toByteArray(path.toUri())))));
    }

    /**
     * Get a path Stream to iterate over all regular files matching extensions in the directory
     *
     * @param directoryPath the path to the directory
     * @param extensions    The extensions of the files to iterate over
     * @return a new Stream object
     */
    private static Stream<Path> directoryStream(@NotNull Path directoryPath, String... extensions) {
        try {
            return StreamSupport.stream(Files.newDirectoryStream(directoryPath).spliterator(), false)
                    .filter(Files::isRegularFile)
                    .filter(path -> Arrays.stream(extensions)
                            .anyMatch(extension -> FilenameUtils
                                    .getExtension(path.getFileName().toString()).toLowerCase()
                                    .equals(extension.toLowerCase())));
        } catch (IOException e) {
            throw new CouchmoveException("Unable to read directory '" + directoryPath + "'", e);
        }
    }
}
