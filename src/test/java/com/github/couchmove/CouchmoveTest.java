package com.github.couchmove;

import com.couchbase.client.java.Bucket;
import com.github.couchmove.exception.CouchmoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.service.ChangeLockService;
import com.github.couchmove.service.ChangeLogDBService;
import com.github.couchmove.service.ChangeLogFileService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static com.github.couchmove.pojo.Status.*;
import static com.github.couchmove.pojo.Type.*;
import static com.github.couchmove.utils.TestUtils.*;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author ctayeb
 *         Created on 04/06/2017
 */
@RunWith(MockitoJUnitRunner.class)
public class CouchmoveTest {

    @InjectMocks
    private Couchmove couchmove = new Couchmove(mockBucket());

    @Mock
    private ChangeLockService lockServiceMock;

    @Mock
    private ChangeLogDBService dbServiceMock;

    @Mock
    private ChangeLogFileService fileServiceMock;

    @Test
    public void should_migration_fail_if_lock_not_acquired() {
        when(lockServiceMock.acquireLock()).thenReturn(false);
        assertThrows(() -> couchmove.migrate(), CouchmoveException.class);
    }

    @Test
    public void should_release_lock_after_migration() throws IOException {
        when(lockServiceMock.acquireLock()).thenReturn(true);
        when(fileServiceMock.fetch()).thenReturn(newArrayList(getRandomChangeLog()));
        when(dbServiceMock.fetchAndCompare(any())).thenReturn(emptyList());
        couchmove.migrate();
        verify(lockServiceMock).releaseLock();
    }

    @Test
    public void should_migration_save_updated_changeLog() {
        ChangeLog changeLog = ChangeLog.builder()
                .version("1")
                .description("update")
                .status(EXECUTED)
                .order(1)
                .build();
        couchmove.executeMigration(newArrayList(changeLog));
        verify(dbServiceMock).save(changeLog);
    }

    @Test
    public void should_migration_skip_unmodified_executed_changeLog() {
        ChangeLog skippedChangeLog = ChangeLog.builder()
                .version("1")
                .status(EXECUTED)
                .order(1)
                .build();
        skippedChangeLog.setCas(RANDOM.nextLong());
        couchmove.executeMigration(newArrayList(skippedChangeLog));
        verify(dbServiceMock, never()).save(any());
    }

    @Test
    public void should_migration_skip_skipped_changeLogs() {
        ChangeLog skippedChangeLog = ChangeLog.builder()
                .version("1")
                .status(SKIPPED)
                .build();
        couchmove.executeMigration(newArrayList(skippedChangeLog));
        verify(dbServiceMock, never()).save(any());
    }

    @Test
    public void should_migration_skip_changeLog_with_old_version() {
        ChangeLog changeLogToSkip = ChangeLog.builder()
                .version("1")
                .description("old version")
                .build();
        ChangeLog executedChangeLog = ChangeLog.builder()
                .version("2")
                .description("new version")
                .order(2)
                .status(EXECUTED)
                .build();
        couchmove.executeMigration(newArrayList(changeLogToSkip, executedChangeLog));
        verify(dbServiceMock).save(changeLogToSkip);
        Assert.assertEquals(SKIPPED, changeLogToSkip.getStatus());
    }

    @Test
    public void should_execute_migrations() {
        Couchmove couchmove = spy(Couchmove.class);
        couchmove.setDbService(dbServiceMock);
        ChangeLog executedChangeLog = ChangeLog.builder()
                .version("1")
                .order(1)
                .status(EXECUTED)
                .build();
        executedChangeLog.setCas(RANDOM.nextLong());
        ChangeLog changeLog = ChangeLog.builder()
                .version("2")
                .description("valid")
                .type(DOCUMENTS)
                .build();
        doNothing().when(couchmove).doExecute(changeLog);
        couchmove.executeMigration(newArrayList(executedChangeLog, changeLog));
        Assert.assertEquals((Integer) 2, changeLog.getOrder());
    }

    @Test
    public void should_throw_exception_if_migration_failed() {
        Couchmove couchmove = spy(Couchmove.class);
        ChangeLog changeLog = ChangeLog.builder()
                .version("1")
                .type(N1QL)
                .build();
        doThrow(CouchmoveException.class).when(couchmove).executeMigration(changeLog, 1);
        assertThrows(() -> couchmove.executeMigration(newArrayList(changeLog)), CouchmoveException.class);
    }

    @Test
    public void should_update_changeLog_on_migration_success() {
        ChangeLog changeLog = ChangeLog.builder()
                .version("1")
                .description("valid change log")
                .type(DESIGN_DOC)
                .build();
        couchmove.executeMigration(changeLog, 1);
        verify(dbServiceMock).save(changeLog);
        Assert.assertNotNull(changeLog.getTimestamp());
        Assert.assertNotNull(changeLog.getDuration());
        Assert.assertNotNull(changeLog.getRunner());
        Assert.assertEquals(EXECUTED, changeLog.getStatus());
    }

    @Test
    public void should_update_changeLog_on_migration_failure() {
        ChangeLog changeLog = ChangeLog.builder()
                .version("1")
                .description("invalid")
                .type(DOCUMENTS)
                .build();
        doThrow(CouchmoveException.class).when(dbServiceMock).importDocuments(any());
        assertThrows(() -> couchmove.executeMigration(changeLog, 1), CouchmoveException.class);
        verify(dbServiceMock).save(changeLog);
        Assert.assertNotNull(changeLog.getTimestamp());
        Assert.assertNotNull(changeLog.getDuration());
        Assert.assertNotNull(changeLog.getRunner());
        Assert.assertEquals(FAILED, changeLog.getStatus());
    }

    @Test
    public void should_execute_failed_changeLog_if_updated() {
        Couchmove couchmove = spy(Couchmove.class);
        couchmove.setDbService(dbServiceMock);
        ChangeLog changeLog = getRandomChangeLog().toBuilder()
                .status(FAILED).build();
        doNothing().when(couchmove).doExecute(changeLog);
        couchmove.executeMigration(newArrayList(changeLog));
        Assert.assertEquals((Integer) 1, changeLog.getOrder());
        Assert.assertEquals(EXECUTED, changeLog.getStatus());
    }

    private static Bucket mockBucket() {
        Bucket mockedBucket = mock(Bucket.class);
        when(mockedBucket.name()).thenReturn("default");
        return mockedBucket;
    }

}