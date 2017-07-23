package com.github.couchmove.pojo;

import com.couchbase.client.java.Bucket;
import lombok.*;
import org.jetbrains.annotations.NotNull;

import java.util.Date;

import static lombok.AccessLevel.PRIVATE;

/**
 * a {@link CouchbaseEntity} representing a change in Couchbase {@link Bucket}
 *
 * @author ctayeb
 *         Created on 27/05/2017
 */
@AllArgsConstructor
@NoArgsConstructor(access = PRIVATE)
@EqualsAndHashCode(callSuper = false)
@Builder(toBuilder = true)
@Data
public class ChangeLog extends CouchbaseEntity implements Comparable<ChangeLog> {

    /**
     * The version of the change
     */
    private String version;

    /**
     * The execution order of the change
     */
    private Integer order;

    /**
     * The description of the change
     */
    private String description;

    /**
     * The {@link Type} of the change
     */
    private Type type;

    /**
     * The script file or folder that was executed in the change
     */
    private String script;

    /**
     * A unique identifier of the file or folder of the change
     */
    private String checksum;

    /**
     * The OS username of the process that executed the change
     */
    private String runner;

    /**
     * Date of execution of the change
     */
    private Date timestamp;

    /**
     * The duration of the execution of the change in milliseconds
     */
    private Long duration;

    /**
     * The {@link Status} of the change
     */
    private Status status;

    @Override
    public int compareTo(@NotNull ChangeLog o) {
        return version == null ? 0 : version.compareTo(o.version);
    }
}
