package com.github.couchmove.service;

import com.github.couchmove.exception.CouchMoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.pojo.Type;
import com.github.couchmove.utils.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.couchmove.pojo.Type.DESIGN_DOC;
import static com.github.couchmove.pojo.Type.N1QL;

/**
 * Created by tayebchlyah on 30/05/2017.
 */
public class ChangeLogFileService {

    private static final Logger logger = LoggerFactory.getLogger(ChangeLogFileService.class);

    private static Pattern fileNamePattern = Pattern.compile("V([\\w.]+)__([\\w ]+)\\.?(\\w*)$");

    private final File changeFolder;

    public ChangeLogFileService(String changePath) {
        this.changeFolder = initializeFolder(changePath);
    }

    public List<ChangeLog> fetch() {
        logger.info("Fetching changeLogs from '{}'", changeFolder.getPath());
        SortedSet<ChangeLog> sortedChangeLogs = new TreeSet<>();
        //noinspection ConstantConditions
        for (File file : changeFolder.listFiles()) {
            String fileName = file.getName();
            Matcher matcher = fileNamePattern.matcher(fileName);
            if (matcher.matches()) {
                ChangeLog changeLog = ChangeLog.builder()
                        .version(matcher.group(1))
                        .script(fileName)
                        .description(matcher.group(2).replace("_", " "))
                        .type(getChangeLogType(file))
                        .checksum(FileUtils.calculateChecksum(file, DESIGN_DOC.getExtension(), N1QL.getExtension()))
                        .build();
                logger.debug("Fetched one : {}", changeLog);
                sortedChangeLogs.add(changeLog);
            }
        }
        logger.info("Fetched {} changeLogs", sortedChangeLogs.size());
        return Collections.unmodifiableList(new ArrayList<>(sortedChangeLogs));
    }

    //<editor-fold desc="Helpers">
    static File initializeFolder(String changePath) {
        Path path;
        try {
            path = FileUtils.getPathFromResource(changePath);
        } catch (FileNotFoundException e) {
            throw new CouchMoveException("The change path '" + changePath + "'doesn't exist");
        } catch (IOException e) {
            throw new CouchMoveException("Unable to get change path '" + changePath + "'", e);
        }
        File file = path.toFile();
        if (!file.isDirectory()) {
            throw new CouchMoveException("The change path '" + changePath + "' is not a directory");
        }
        return file;
    }

    @NotNull
    static Type getChangeLogType(File file) {
        if (file.isDirectory()) {
            return Type.DOCUMENTS;
        } else {
            String extension = FilenameUtils.getExtension(file.getName()).toLowerCase();
            if (DESIGN_DOC.getExtension().equals(extension)) {
                return DESIGN_DOC;
            }
            if (N1QL.getExtension().equals(extension)) {
                return N1QL;
            }
        }
        throw new CouchMoveException("Unknown ChangeLog type : " + file.getName());
    }

    //</editor-fold>

}
