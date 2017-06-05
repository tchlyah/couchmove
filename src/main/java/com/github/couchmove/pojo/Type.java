package com.github.couchmove.pojo;

import lombok.Getter;

/**
 * Created by tayebchlyah on 27/05/2017.
 */
public enum Type {
    DOCUMENTS(""),
    DESIGN_DOC(Constants.JSON),
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
