package com.github.couchmove.repository;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.*;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.*;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.manager.collection.*;
import com.couchbase.client.java.manager.query.BuildQueryIndexOptions;
import com.couchbase.client.java.manager.query.QueryIndex;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.manager.view.DesignDocument;
import com.couchbase.client.java.manager.view.View;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.view.DesignDocumentNamespace;
import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.CouchbaseEntity;
import lombok.*;
import org.apache.commons.lang.text.StrSubstitutor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.manager.collection.CreateCollectionOptions.createCollectionOptions;
import static com.couchbase.client.java.manager.collection.CreateScopeOptions.createScopeOptions;
import static com.couchbase.client.java.manager.collection.GetAllScopesOptions.getAllScopesOptions;
import static com.couchbase.client.java.manager.query.BuildQueryIndexOptions.buildDeferredQueryIndexesOptions;
import static com.couchbase.client.java.manager.query.GetAllQueryIndexesOptions.getAllQueryIndexesOptions;
import static com.couchbase.client.java.manager.query.WatchQueryIndexesOptions.watchQueryIndexesOptions;
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
    public static final String SCOPE_PARAM = "scope";
    public static final int MAX_ATTEMPTS = 5;
    public static final String DEFAULT = "_default";

    private final Bucket bucket;

    private final Cluster cluster;

    private final Collection collection;

    private final Class<E> entityClass;

    public CouchbaseRepositoryImpl(Cluster cluster, Collection collection, Class<E> entityClass) {
        this.cluster = cluster;
        this.bucket = cluster.bucket(collection.bucketName());
        this.collection = getOrCreate(collection);
        this.entityClass = entityClass;
    }

    private Collection getOrCreate(Collection collection) {
        String scopeName = collection.scopeName();
        String collectionName = collection.name();

        CollectionManager collections = this.bucket.collections();

        Optional<ScopeSpec> scope = collections
                .getAllScopes(withRetry(getAllScopesOptions())).stream()
                .filter(s -> s.name().equals(scopeName))
                .findFirst();
        Optional<CollectionSpec> collectionSpec = Optional.empty();
        if (!scope.isPresent()) {
            collections.createScope(scopeName, withRetry(createScopeOptions()));
        } else {
            ScopeSpec scopeSpec = scope.get();
            collectionSpec = scopeSpec.collections().stream()
                    .filter(c -> c.name().equals(collectionName))
                    .findFirst();
        }
        if (!collectionSpec.isPresent()) {
            collections.createCollection(CollectionSpec.create(collectionName, scopeName), withRetry(createCollectionOptions()));
        }

        return collection;
    }

    public CouchbaseRepositoryImpl(Cluster cluster, Bucket bucket, Class<E> entityClass) {
        this.cluster = cluster;
        this.bucket = bucket;
        this.collection = bucket.defaultCollection();
        this.entityClass = entityClass;
    }

    @Override
    public CouchbaseRepositoryImpl<E> withCollection(String collection) {
        return new CouchbaseRepositoryImpl<>(cluster, bucket.scope(this.collection.scopeName()).collection(collection), entityClass);
    }

    @Override
    public CouchbaseRepositoryImpl<E> withCollection(String scope, String collection) {
        return new CouchbaseRepositoryImpl<>(cluster, bucket.scope(scope).collection(collection), entityClass);
    }

    @Override
    public E save(String id, E entity) {
        logger.trace("Save entity '{}' with id '{}'", entity, id);
        try {
            MutationResult insertedDocument = collection.upsert(id, entity);
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
            insertedDocument = collection.replace(id, entity, withRetry(replaceOptions().cas(entity.getCas())));
        } else {
            insertedDocument = collection.insert(id, entity, withRetry(insertOptions()));
        }
        entity.setCas(insertedDocument.cas());
        return entity;
    }

    @Override
    public void delete(String id) {
        logger.trace("Remove entity with id '{}'", id);
        try {
            collection.remove(id);
        } catch (DocumentNotFoundException e) {
            logger.debug("Trying to delete document that does not exist : '{}'", id);
        }
    }

    @Override
    public E findOne(String id) {
        logger.trace("Find entity with id '{}'", id);
        try {
            GetResult document = collection.get(id, withRetry(GetOptions.getOptions()));
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
        collection.upsert(id, jsonContent, withRetry(UpsertOptions.upsertOptions().transcoder(RawJsonTranscoder.INSTANCE)));
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
            retry(() -> cluster.query(parametrizedStatement, withRetry(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS))),
                    "12003", "12021");
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
            retry(() -> cluster.searchIndexes().upsertIndex(searchIndex, withRetry(upsertSearchIndexOptions())), "doesn't belong to scope");
        } catch (CouchbaseException | JsonProcessingException e) {
            throw new CouchmoveException("Could not store FTS index '" + name + "'", e);
        }
    }

    public Optional<SearchIndex> getFtsIndex(String name) {
        try {
            return Optional.of(cluster.searchIndexes().getIndex(name));
        } catch (IndexNotFoundException e) {
            return Optional.empty();
        }
    }

    @Override
    public boolean isFtsIndexExists(String name) {
        return getFtsIndex(name).isPresent();
    }

    String injectParameters(String statement) {
        return StrSubstitutor.replace(statement, of(
                BUCKET_PARAM, getBucketName(),
                SCOPE_PARAM, getScopeName()
        ));
    }

    @Override
    public String getBucketName() {
        return collection.bucketName();
    }

    @Override
    public String getScopeName() {
        return collection.scopeName();
    }

    @Override
    public void buildN1qlDeferredIndexes() {
        logger.info("Build N1QL Deferred Indexes for default");
        buildN1qlDeferredIndexes(collection.scopeName(), collection.name());
    }

    @Override
    public void buildN1qlDeferredIndexes(String scope, String collection) {
        BuildQueryIndexOptions buildQueryIndexOptions = withRetry(buildDeferredQueryIndexesOptions());
        if (!DEFAULT.equals(scope) || !DEFAULT.equals(collection)) {
            buildQueryIndexOptions = buildQueryIndexOptions.scopeName(scope).collectionName(collection);
        }
        try {
            cluster.queryIndexes().buildDeferredIndexes(getBucketName(), buildQueryIndexOptions);
        } catch (CouchbaseException e) {
            if (e.getMessage().contains("building in the background")) {
                logger.warn("Build Index failed, will retry building in the background");
            } else {
                throw e;
            }
        }
    }

    @Override
    public void buildN1qlDeferredIndexes(String scope) {
        logger.info("Build N1QL Deferred Indexes for scope '{}'", scope);
        bucket.collections().getAllScopes(withRetry(getAllScopesOptions())).stream()
                .filter(scopeSpec -> scopeSpec.name().equals(scope))
                .flatMap(scopeSpec -> scopeSpec.collections().stream())
                .map(CollectionSpec::name)
                .forEach(collection -> buildN1qlDeferredIndexes(scope, collection));
    }

    @Override
    public void watchN1qlIndexes(Duration duration) {
        logger.info("Waiting for {} for N1QL indexes to be ready", duration.toString().replace("PT", ""));
        watchN1qlIndexes(collection.scopeName(), collection.name(), duration);
    }

    @Override
    public void watchN1qlIndexes(String scope, String collection, Duration duration) {
        var getAllIndexesOptions = withRetry(getAllQueryIndexesOptions());
        var watchQueryIndexesOptions = watchQueryIndexesOptions();
        if (!DEFAULT.equals(collection) || !DEFAULT.equals(scope)) {
            getAllIndexesOptions = getAllIndexesOptions.scopeName(scope).collectionName(collection);
            watchQueryIndexesOptions = watchQueryIndexesOptions.scopeName(scope).collectionName(collection);
        }
        List<String> indexes = cluster.queryIndexes().getAllIndexes(getBucketName(), getAllIndexesOptions).stream()
                .map(QueryIndex::name)
                .collect(Collectors.toList());
        cluster.queryIndexes().watchIndexes(getBucketName(), indexes, duration, watchQueryIndexesOptions);
    }

    @Override
    public void watchN1qlIndexes(String scope, Duration duration) {
        logger.info("Waiting for {} for N1QL indexes in scope {} to be ready", duration.toString().replace("PT", ""), scope);
        bucket.collections().getAllScopes(withRetry(getAllScopesOptions())).stream()
                .filter(scopeSpec -> scopeSpec.name().equals(scope))
                .flatMap(scopeSpec -> scopeSpec.collections().stream())
                .map(CollectionSpec::name)
                .forEach(collection -> watchN1qlIndexes(scope, collection, duration));
    }

    private static <SELF extends CommonOptions<SELF>> SELF withRetry(SELF options) {
        return options.retryStrategy(BestEffortRetryStrategy.INSTANCE);
    }

    private static void retry(Runnable runnable, String... errorContains) {
        Mono
                .defer(() -> Mono.fromRunnable(runnable))
                .retryWhen(Retry.backoff(MAX_ATTEMPTS, Duration.ofMillis(500))
                        .filter(t -> Arrays.stream(errorContains).anyMatch(e -> t.getMessage().contains(e)))
                        .doBeforeRetry(retrySignal ->
                                logger.debug("Error while executing request, retrying {}/{}", retrySignal.totalRetries(), MAX_ATTEMPTS)))
                .block();
    }

}
