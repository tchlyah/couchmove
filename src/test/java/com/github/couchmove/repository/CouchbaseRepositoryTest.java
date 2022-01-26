package com.github.couchmove.repository;

import com.couchbase.client.java.manager.eventing.*;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.github.couchmove.utils.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.time.temporal.ChronoUnit;

import static com.github.couchmove.utils.FileUtilsTest.SUCCESS_PATH;
import static org.assertj.core.api.Assertions.assertThat;

class CouchbaseRepositoryTest {

    @Test
    void should_read_eventing_function() throws Exception {
        String jsonContent = IOUtils.toString(FileUtils.getPathFromResource(SUCCESS_PATH + "V3__test.eventing").toUri(), Charset.defaultCharset());

        EventingFunction[] eventingFunctions = CouchbaseRepositoryImpl.getJsonMapper().readValue(jsonContent, EventingFunction[].class);
        assertThat(eventingFunctions)
                .hasSize(1)
                .allSatisfy(eventingFunction -> {
                    assertThat(eventingFunction.name()).isEqualTo("test");
                    assertThat(eventingFunction.code()).contains("function OnUpdate(doc, meta) {");
                    assertThat(eventingFunction.sourceKeyspace())
                            .satisfies(sourceKeyspace -> {
                                assertThat(sourceKeyspace.bucket()).isEqualTo("${bucket}");
                                assertThat(sourceKeyspace.collection()).isEqualTo("_default");
                                assertThat(sourceKeyspace.scope()).isEqualTo("_default");
                            });
                    assertThat(eventingFunction.metadataKeyspace())
                            .satisfies(metadataKeyspace -> {
                                assertThat(metadataKeyspace.bucket()).isEqualTo("eventing");
                                assertThat(metadataKeyspace.scope()).isEqualTo("_default");
                                assertThat(metadataKeyspace.collection()).isEqualTo("_default");
                            });
                    assertThat(eventingFunction.settings())
                            .satisfies(settings -> {
                                assertThat(settings.dcpStreamBoundary()).isEqualTo(EventingFunctionDcpBoundary.EVERYTHING);
                                assertThat(settings.deploymentStatus().isDeployed()).isFalse();
                                assertThat(settings.executionTimeout().get(ChronoUnit.SECONDS)).isEqualTo(60);
                                assertThat(settings.languageCompatibility()).isEqualTo(EventingFunctionLanguageCompatibility.VERSION_6_6_2);
                                assertThat(settings.logLevel()).isEqualTo(EventingFunctionLogLevel.INFO);
                                assertThat(settings.queryConsistency()).isEqualTo(QueryScanConsistency.NOT_BOUNDED);
                                assertThat(settings.numTimerPartitions()).isEqualTo(128);
                                assertThat(settings.processingStatus().isRunning()).isFalse();
                                assertThat(settings.timerContextSize()).isEqualTo(1024);
                                assertThat(settings.userPrefix()).isEqualTo("eventing");
                                assertThat(settings.workerCount()).isEqualTo(1);
                            });
                    assertThat(eventingFunction.enforceSchema()).isFalse();
                    assertThat(eventingFunction.version()).isEqualTo("evt-7.0.3-7031-ee");
                    assertThat(eventingFunction.bucketBindings())
                            .hasSize(1)
                            .allSatisfy(bucketBinding -> {
                                assertThat(bucketBinding.alias()).isEqualTo("destination");
                                assertThat(bucketBinding.keyspace())
                                        .satisfies(keyspace -> {
                                            assertThat(keyspace.bucket()).isEqualTo("${bucket}");
                                            assertThat(keyspace.scope()).isEqualTo("_default");
                                            assertThat(keyspace.scope()).isEqualTo("_default");
                                        });
                                assertThat(bucketBinding.access()).isEqualTo(EventingFunctionBucketAccess.READ_ONLY);
                            });
                });
    }
}
