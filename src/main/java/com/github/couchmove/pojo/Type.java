package com.github.couchmove.pojo;

import lombok.Getter;

/**
 * Created by tayebchlyah on 27/05/2017.
 */
public enum Type {
    DOCUMENTS(""),
    DESIGN_DOC("json"),
    N1QL("n1ql");

    @Getter
    private final String extension;

    Type(String extension) {
        this.extension = extension;
    }
}
