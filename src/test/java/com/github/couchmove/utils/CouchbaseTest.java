package com.github.couchmove.utils;

import org.junit.jupiter.api.AfterEach;
import org.testcontainers.couchbase.AbstractCouchbaseTest;
import org.testcontainers.couchbase.CouchbaseContainer;

public abstract class CouchbaseTest extends AbstractCouchbaseTest {

    private static CouchbaseContainer container = initCouchbaseContainer("couchbase:6.5.0").withFts(true);

    @Override
    protected CouchbaseContainer getCouchbaseContainer() {
        container.start();
        return container;
    }

    @AfterEach
    public void clear() {
        super.clear();
    }
}
