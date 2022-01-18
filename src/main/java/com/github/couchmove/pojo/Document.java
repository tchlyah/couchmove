package com.github.couchmove.pojo;

import lombok.Value;

@Value
public class Document {

    String scope;

    String collection;

    String fileName;

    String content;
}
