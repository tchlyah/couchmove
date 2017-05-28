package com.github.couchmove.pojo;

import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.jetbrains.annotations.Nullable;

/**
 * Created by tayebchlyah on 28/05/2017.
 */
@Data
public class CouchbaseEntity {

    @Nullable
    @JsonIgnore
    private Long cas;
}
