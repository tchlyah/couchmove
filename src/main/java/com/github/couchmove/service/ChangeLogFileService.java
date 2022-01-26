package com.github.couchmove.service;

import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.*;
import com.github.couchmove.utils.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.github.couchmove.pojo.Type.Constants.JSON;
import static com.github.couchmove.pojo.Type.*;
import static java.nio.file.Files.newDirectoryStream;

/**
 * Service for fetching {@link ChangeLog}s from resource folder
 *
 * @author ctayeb
 * Created on 30/05/2017
 */
public class ChangeLogFileService {

    private static final Logger logger = LoggerFactory.getLogger(ChangeLogFileService.class);

    private static Pattern fileNamePattern = Pattern.compile("V([\\w.]+)__([\\w ]+)\\.?(\\w*)/?$");

    private final Path changePath;

    /**
     * @param changePath The resource path of the folder containing {@link ChangeLog}s
     */
    public ChangeLogFileService(String changePath) {
        this.changePath = initializeFolder(changePath);
    }

    /**
     * Reads all the {@link ChangeLog}s contained in the Change Folder, ignoring unhandled files
     *
     * @return An ordered list of {@link ChangeLog}s by {@link ChangeLog#version}
     * @throws IOException if unable to open changePath
     */
    public List<ChangeLog> fetch() throws IOException {
        logger.info("Reading from migration folder '{}'", changePath);
        List<ChangeLog> changelogs = StreamSupport.stream(newDirectoryStream(changePath).spliterator(), false)
                .map(path -> {
                    String fileName = path.getFileName().toString();
                    Matcher matcher = fileNamePattern.matcher(fileName);
                    if (!matcher.matches()) {
                        return null;
                    }
                    return ChangeLog.builder()
                            .version(matcher.group(1))
                            .script(fileName)
                            .description(matcher.group(2).replace("_", " "))
                            .type(getChangeLogType(path))
                            .checksum(FileUtils.calculateChecksum(path, DESIGN_DOC.getExtension(), N1QL.getExtension(), Type.FTS.getExtension(), EVENTING.getExtension()))
                            .build();
                })
                .filter(Objects::nonNull)
                .peek(changelog -> logger.debug("Fetched one : {}", changelog))
                .sorted()
                .collect(Collectors.toList());
        logger.info("Fetched {} change logs from migration folder", changelogs.size());
        return Collections.unmodifiableList(changelogs);
    }

    /**
     * Read file content from a relative path from the Change Folder
     *
     * @param path relative path of the file to read
     * @return content of the file
     * @throws IOException if an I/O error occurs reading the file
     */
    public String readFile(String path) throws IOException {
        return new String(IOUtils.toByteArray(resolve(path).toUri()));
    }

    /**
     * Read json files content from a relative directory from the Change Folder
     *
     * @param path relative path of the directory containing json files to read
     * @return {@link Map} which keys represents the name (with extension), and values the content of read files
     * @throws IOException if an I/O error occurs reading the files
     */
    public Collection<Document> readDocuments(String path) throws IOException {
        return FileUtils.readFilesInDirectory(resolve(path), JSON);
    }

    //<editor-fold desc="Helpers">
    static Path initializeFolder(String changePath) {
        Path path;
        try {
            path = FileUtils.getPathFromResource(changePath);
        } catch (FileNotFoundException e) {
            throw new CouchmoveException("The change path '" + changePath + "'doesn't exist");
        } catch (IOException e) {
            throw new CouchmoveException("Unable to get change path '" + changePath + "'", e);
        }
        if (!Files.isDirectory(path)) {
            throw new CouchmoveException("The change path '" + changePath + "' is not a directory");
        }
        return path;
    }

    private Path resolve(String path) {
        return changePath.resolve(path);
    }

    /**
     * Determines the {@link Type} of the file from its type and extension
     *
     * @param path file to analyse
     * @return {@link Type} of the {@link ChangeLog} file
     */
    @NotNull
    public static Type getChangeLogType(Path path) {
        if (Files.isDirectory(path)) {
            return Type.DOCUMENTS;
        }
        return Type.fromExtension(FilenameUtils.getExtension(path.getFileName().toString()).toLowerCase());
    }
    //</editor-fold>

}
