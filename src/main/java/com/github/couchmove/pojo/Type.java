package com.github.couchmove.pojo;

import com.github.couchmove.exception.CouchmoveException;
import lombok.Getter;

import java.util.Arrays;

/**
 * Describes the type of the {@link ChangeLog}
 *
 * @author ctayeb
 * Created on 27/05/2017
 */
public enum Type {

    /**
     * json documents
     */
    DOCUMENTS(""),

    /**
     * json document representing a {@link com.couchbase.client.java.manager.view.DesignDocument}
     */
    DESIGN_DOC(Constants.JSON),

    /**
     * n1ql file containing a list of N1ql queries
     */
    N1QL(Constants.N1QL),

    /**
     * fts json file containing Full Text Search index definition
     */
    FTS(Constants.FTS),

    /**
     * Eventing json file containing Function definition
     */
    EVENTING(Constants.EVENTING);

    @Getter
    private final String extension;

    Type(String extension) {
        this.extension = extension;
    }

    public static class Constants {
        public static final String JSON = "json";
        public static final String N1QL = "n1ql";
        public static final String FTS = "fts";
        public static final String EVENTING = "eventing";
    }

    public static Type fromExtension(String extension) {
        return Arrays.stream(Type.values())
                .filter(t -> t.getExtension().equals(extension))
                .findFirst()
                .orElseThrow(() -> new CouchmoveException("Unknown ChangeLog type : " + extension));
    }
}
