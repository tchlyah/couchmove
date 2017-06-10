package com.github.couchmove.pojo;

import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.view.DesignDocument;
import lombok.Getter;

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
     * json document representing a {@link DesignDocument}
     */
    DESIGN_DOC(Constants.JSON),

    /**
     * n1ql file containing a list of {@link N1qlQuery}
     */
    N1QL(Constants.N1QL);

    @Getter
    private final String extension;

    Type(String extension) {
        this.extension = extension;
    }

    public static class Constants {
        public static final String JSON = "json";
        public static final String N1QL = "n1ql";
    }
}
