package com.github.couchmove.pojo;

import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.jetbrains.annotations.Nullable;

/**
 * Class representing a json Document
 *
 * @author ctayeb
 * Created on 28/05/2017
 */
@Data
public class CouchbaseEntity {

    /**
     * The last-known CAS value for the Document
     * <p>
     * CAS is for Check And Swap, which is an identifier that permit optimistic concurrency
     */
    @Nullable
    @JsonIgnore
    private Long cas;
}
