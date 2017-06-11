package com.github.couchmove.container;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import lombok.Getter;
import org.junit.After;
import org.slf4j.LoggerFactory;

/**
 * @author ctayeb
 */
public abstract class AbstractCouchbaseTest {
    public static final String CLUSTER_USER = "Administrator";
    public static final String CLUSTER_PASSWORD = "password";
    public static final String DEFAULT_BUCKET = "default";

    static {
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.INFO);
    }

    @Getter(lazy = true)
    private final static CouchbaseContainer couchbaseContainer = initCouchbaseContainer();

    @Getter(lazy = true)
    private final static Bucket bucket = openBucket(DEFAULT_BUCKET);

    @After
    public void clear() {
        getBucket().bucketManager().flush();
    }

    private static CouchbaseContainer initCouchbaseContainer() {
        CouchbaseContainer couchbaseContainer = new CouchbaseContainer()
                .withFTS(false)
                .withIndex(true)
                .withQuery(true)
                .withPrimaryIndex(false)
                .withClusterUsername(CLUSTER_USER)
                .withClusterPassword(CLUSTER_PASSWORD);
        couchbaseContainer.start();
        couchbaseContainer.createBucket(DefaultBucketSettings.builder()
                .enableFlush(true)
                .name(DEFAULT_BUCKET)
                .quota(100)
                .replicas(0)
                .port(couchbaseContainer.getMappedPort(CouchbaseContainer.BUCKET_PORT))
                .type(BucketType.COUCHBASE)
                .build(), false);
        return couchbaseContainer;
    }

    private static Bucket openBucket(String bucketName) {
        CouchbaseCluster cluster = getCouchbaseContainer().getCouchbaseCluster();
        Bucket bucket = cluster.openBucket(bucketName);
        Runtime.getRuntime().addShutdownHook(new Thread(bucket::close));
        return bucket;
    }
}
