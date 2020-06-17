package com.github.couchmove.utils;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.search.SearchIndexManager;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

public abstract class BaseIT {

    private static final String TEST_BUCKET = "test";

    @Getter(lazy = true)
    private static final CouchbaseContainer couchbaseContainer = initCouchbaseContainer("couchbase:6.5.0");

    @Getter(lazy = true)
    private static final Cluster cluster = initCluster();

    @Getter(lazy = true)
    private static final Bucket bucket = getCluster().bucket(TEST_BUCKET);

    @AfterEach
    public void clear() {
        getCluster().query(String.format("DELETE FROM `%s`", getBucket().name()), QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS));
    }

    private static CouchbaseContainer initCouchbaseContainer(String imageName) {
        CouchbaseContainer container = new CouchbaseContainer(imageName)
                .withBucket(new BucketDefinition(TEST_BUCKET));
        container.start();
        return container;
    }

    @NotNull
    private static Cluster initCluster() {
        return Cluster.connect(getCouchbaseContainer().getConnectionString(), getCouchbaseContainer().getUsername(), getCouchbaseContainer().getPassword());
    }
}
