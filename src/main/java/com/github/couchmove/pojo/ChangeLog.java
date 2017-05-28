package com.github.couchmove.pojo;

import lombok.Builder;
import lombok.Data;

import java.util.Date;

/**
 * Created by tayebchlyah on 27/05/2017.
 */
@Builder
@Data
public class ChangeLog extends CouchbaseEntity {

    private String version;

    private String description;

    private Type type;

    private int checksum;

    private String runner;

    private Date timestamp;

    private long duration;

    private boolean success;
}
