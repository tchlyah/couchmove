package com.github.couchmove.pojo;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.jetbrains.annotations.NotNull;

import java.util.Date;

/**
 * Created by tayebchlyah on 27/05/2017.
 */
@EqualsAndHashCode(callSuper = false)
@Builder(toBuilder = true)
@Data
public class ChangeLog extends CouchbaseEntity implements Comparable<ChangeLog> {

    private String version;

    private Integer order;

    private String description;

    private Type type;

    private String script;

    private String checksum;

    private String runner;

    private Date timestamp;

    private Long duration;

    private Status status;

    @Override
    public int compareTo(@NotNull ChangeLog o) {
        return version == null ? 0 : version.compareTo(o.version);
    }
}
