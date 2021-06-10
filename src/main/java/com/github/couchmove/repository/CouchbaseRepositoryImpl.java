package com.github.couchmove.repository;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.manager.query.QueryIndex;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.manager.view.DesignDocument;
import com.couchbase.client.java.manager.view.View;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.view.DesignDocumentNamespace;
import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.CouchbaseEntity;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang.text.StrSubstitutor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.manager.search.UpsertSearchIndexOptions.upsertSearchIndexOptions;
import static com.google.common.collect.ImmutableMap.of;
import static lombok.AccessLevel.PACKAGE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author ctayeb
 * Created on 27/05/2017
 */
// For tests
@SuppressWarnings("ConstantConditions")
@NoArgsConstructor(access = PACKAGE, force = true)
@RequiredArgsConstructor
public class CouchbaseRepositoryImpl<E extends CouchbaseEntity> implements CouchbaseRepository<E> {

    private static final Logger logger = getLogger(CouchbaseRepositoryImpl.class);

    @Getter(lazy = true)
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    public static final String BUCKET_PARAM = "bucket";

    private final Bucket bucket;

    private final Cluster cluster;

    @Getter(lazy = true)
    private final Collection collection = bucket.defaultCollection();

    private final Class<E> entityClass;

    @Override
    public E save(String id, E entity) {
        logger.trace("Save entity '{}' with id '{}'", entity, id);
        try {
            MutationResult insertedDocument = getCollection().upsert(id, entity);
            entity.setCas(insertedDocument.cas());
            return entity;
        } catch (CouchbaseException e) {
            throw new CouchmoveException("Unable to save document with id " + id, e);
        }
    }

    @Override
    public E checkAndSave(String id, E entity) {
        logger.trace("Check and save entity '{}' with id '{}'", entity, id);
        MutationResult insertedDocument;
        if (entity.getCas() != null) {
            insertedDocument = getCollection().replace(id, entity, withRetry(replaceOptions().cas(entity.getCas())));
        } else {
            insertedDocument = getCollection().insert(id, entity, withRetry(insertOptions()));
        }
        entity.setCas(insertedDocument.cas());
        return entity;
    }

    @Override
    public void delete(String id) {
        logger.trace("Remove entity with id '{}'", id);
        try {
            getCollection().remove(id);
        } catch (DocumentNotFoundException e) {
            logger.debug("Trying to delete document that does not exist : '{}'", id);
        }
    }

    @Override
    public E findOne(String id) {
        logger.trace("Find entity with id '{}'", id);
        try {
            GetResult document = getCollection().get(id, withRetry(GetOptions.getOptions()));
            E entity = document.contentAs(entityClass);
            entity.setCas(document.cas());
            return entity;
        } catch (DocumentNotFoundException e) {
            return null;
        } catch (CouchbaseException e) {
            throw new CouchmoveException("Unable to read document with id " + id, e);
        }
    }

    @Override
    public void save(String id, String jsonContent) {
        logger.trace("Save document with id '{}' : \n'{}'", id, jsonContent);
        getCollection().upsert(id, jsonContent, withRetry(UpsertOptions.upsertOptions().transcoder(RawJsonTranscoder.INSTANCE)));
    }

    @Override
    public void importDesignDoc(String name, String jsonContent) {
        logger.trace("Import document : \n'{}'", jsonContent);
        bucket.viewIndexes().upsertDesignDocument(toDesignDocument(name, jsonContent), DesignDocumentNamespace.PRODUCTION);
    }

    @NotNull
    private static DesignDocument toDesignDocument(String name, String jsonContent) {
        JsonNode node = Mapper.decodeIntoTree(jsonContent.getBytes());
        ObjectNode viewsNode = (ObjectNode) node.path("views");
        Map<String, View> views = Mapper.convertValue(viewsNode, new TypeReference<Map<String, View>>() {
        });
        return new DesignDocument(name, views);
    }

    @Override
    public void query(String n1qlStatement) {
        String parametrizedStatement = injectParameters(n1qlStatement);
        logger.debug("Execute n1ql request : \n{}", parametrizedStatement);
        try {
            cluster.query(parametrizedStatement, withRetry(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS)));
        } catch (Exception e) {
            throw new CouchmoveException("Unable to execute n1ql request", e);
        }
    }

    @Override
    public void importFtsIndex(String name, String jsonContent) {
        jsonContent = injectParameters(jsonContent);
        logger.trace("Import FTS index : \n'{}'", jsonContent);
        try {
            CustomSearchIndex searchIndex = getJsonMapper().readValue(jsonContent, CustomSearchIndex.class);
            cluster.searchIndexes().upsertIndex(searchIndex, withRetry(upsertSearchIndexOptions()));
        } catch (CouchbaseException | JsonProcessingException e) {
            throw new CouchmoveException("Could not store FTS index '" + name + "'", e);
        }
    }

    @Override
    public boolean isFtsIndexExists(String name) {
        try {
            cluster.searchIndexes().getIndex(name);
            return true;
        } catch (IndexNotFoundException e) {
            return false;
        }
    }

    String injectParameters(String statement) {
        return StrSubstitutor.replace(statement, of(BUCKET_PARAM, getBucketName()));
    }

    @Override
    public String getBucketName() {
        return getCollection().bucketName();
    }

    @Override
    public void buildN1qlDeferredIndexes() {
        cluster.queryIndexes().buildDeferredIndexes(getBucketName());
    }

    @Override
    public void watchN1qlIndexes(Duration duration) {
        List<String> indexes = cluster.queryIndexes().getAllIndexes(getBucketName()).stream()
                .map(QueryIndex::name)
                .collect(Collectors.toList());
        cluster.queryIndexes().watchIndexes(getBucketName(), indexes, duration);
    }

    private static <SELF extends CommonOptions<SELF>> SELF withRetry(SELF options) {
        return options.retryStrategy(BestEffortRetryStrategy.INSTANCE);
    }

}
