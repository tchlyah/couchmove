package com.github.couchmove.pojo;

import lombok.Builder;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.util.Date;

/**
 * Created by tayebchlyah on 27/05/2017.
 */
@Builder
@Data
public class ChangeLog extends CouchbaseEntity implements Comparable<ChangeLog> {

    private String version;

    private int order;

    private String description;

    private Type type;

    private String script;

    private String checksum;

    private String runner;

    private Date timestamp;

    private long duration;

    private boolean success;

    @Override
    public int compareTo(@NotNull ChangeLog o) {
        return version == null ? 0 : version.compareTo(o.version);
    }
}
