package com.github.couchmove.repository;

import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.json.Mapper;

public class CustomSearchIndex extends SearchIndex {

    @JsonCreator
    public CustomSearchIndex(
        @JsonProperty("uuid") String uuid,
        @JsonProperty("name") String name,
        @JsonProperty("type") String type,
        @JsonProperty("params") Map<String, Object> params,
        @JsonProperty("sourceUUID") String sourceUuid,
        @JsonProperty("sourceName") String sourceName,
        @JsonProperty("sourceParams") Map<String, Object> sourceParams,
        @JsonProperty("sourceType") String sourceType,
        @JsonProperty("planParams") Map<String, Object> planParams) {
        super(uuid, name, type, params, sourceUuid, sourceName, sourceParams, sourceType, planParams);
    }

    @Override
    public String toJson() {
        Map<String, Object> output = new HashMap<>();

        if (super.uuid() != null) {
            output.put("uuid", super.uuid());
        }
        output.put("name", super.name());
        output.put("sourceName", super.sourceName());
        output.put("type", super.type() == null ? "fulltext-index" : super.type());
        output.put("sourceType", super.sourceType() == null ? "couchbase" : super.sourceType());
        output.put("params", super.params());
        output.put("planParams", super.planParams());
        output.put("sourceUUID", super.sourceUuid());

        return Mapper.encodeAsString(output);
    }    
    
}
