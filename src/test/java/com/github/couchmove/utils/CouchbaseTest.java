package com.github.couchmove.utils;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

public abstract class CouchbaseTest {

    private static final String TEST_BUCKET = "test";

    @Getter(lazy = true)
    private static final CouchbaseContainer couchbaseContainer = initCouchbaseContainer("couchbase:6.5.0");

    @Getter(lazy = true)
    private static final CouchbaseEnvironment couchbaseEnvironment = initCouchbaseEnvironment();

    @Getter(lazy = true)
    private static final CouchbaseCluster couchbaseCluster = initCluster();

    @Getter(lazy = true)
    private static final Bucket bucket = getCouchbaseCluster().openBucket(TEST_BUCKET);

    @AfterEach
    public void clear() {
        getBucket().query(
                N1qlQuery.simple(String.format("DELETE FROM `%s`", getBucket().name()),
                        N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)));
    }

    private static CouchbaseContainer initCouchbaseContainer(String imageName) {
        CouchbaseContainer container = new CouchbaseContainer(imageName)
                .withBucket(new BucketDefinition(TEST_BUCKET));
        container.start();
        return container;
    }

    private static DefaultCouchbaseEnvironment initCouchbaseEnvironment() {
        return DefaultCouchbaseEnvironment.builder()
                .bootstrapCarrierDirectPort(getCouchbaseContainer().getBootstrapCarrierDirectPort())
                .bootstrapHttpDirectPort(getCouchbaseContainer().getBootstrapHttpDirectPort())
                .build();
    }

    @NotNull
    private static CouchbaseCluster initCluster() {
        CouchbaseCluster couchbaseCluster = CouchbaseCluster.create(getCouchbaseEnvironment(), getCouchbaseContainer().getContainerIpAddress());
        couchbaseCluster.authenticate(getCouchbaseContainer().getUsername(), getCouchbaseContainer().getPassword());
        return couchbaseCluster;
    }
}
