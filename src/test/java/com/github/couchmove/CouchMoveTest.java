package com.github.couchmove;

import com.couchbase.client.java.Bucket;
import com.github.couchmove.exception.CouchMoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.pojo.Status;
import com.github.couchmove.pojo.Type;
import com.github.couchmove.service.ChangeLockService;
import com.github.couchmove.service.ChangeLogDBService;
import com.github.couchmove.service.ChangeLogFileService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.github.couchmove.pojo.Status.EXECUTED;
import static com.github.couchmove.pojo.Status.SKIPPED;
import static com.github.couchmove.utils.TestUtils.RANDOM;
import static com.github.couchmove.utils.TestUtils.getRandomChangeLog;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by tayebchlyah on 04/06/2017.
 */
@RunWith(MockitoJUnitRunner.class)
public class CouchMoveTest {

    @InjectMocks
    private CouchMove couchMove = new CouchMove(mockBucket());

    @Mock
    private ChangeLockService lockServiceMock;

    @Mock
    private ChangeLogDBService dbServiceMock;

    @Mock
    private ChangeLogFileService fileServiceMock;

    @Test(expected = CouchMoveException.class)
    public void should_migration_fail_if_lock_not_acquired() {
        when(lockServiceMock.acquireLock()).thenReturn(false);
        couchMove.migrate();
    }

    @Test
    public void should_release_lock_after_migration() {
        when(lockServiceMock.acquireLock()).thenReturn(true);
        when(fileServiceMock.fetch()).thenReturn(newArrayList(getRandomChangeLog()));
        when(dbServiceMock.fetchAndCompare(any())).thenReturn(emptyList());
        couchMove.migrate();
        verify(lockServiceMock).releaseLock();
    }

    @Test
    public void should_migration_save_updated_changeLog() {
        ChangeLog changeLog = ChangeLog.builder()
                .version("1")
                .status(EXECUTED)
                .order(1)
                .build();
        couchMove.executeMigration(newArrayList(changeLog));
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
        couchMove.executeMigration(newArrayList(skippedChangeLog));
        verify(dbServiceMock, never()).save(any());
    }

    @Test
    public void should_migration_skip_skipped_changeLogs() {
        ChangeLog skippedChangeLog = ChangeLog.builder()
                .version("1")
                .status(SKIPPED)
                .build();
        couchMove.executeMigration(newArrayList(skippedChangeLog));
        verify(dbServiceMock, never()).save(any());
    }

    @Test
    public void should_migration_skip_changeLog_with_old_version() {
        ChangeLog changeLogToSkip = ChangeLog.builder()
                .version("1")
                .build();
        ChangeLog executedChangeLog = ChangeLog.builder()
                .version("2")
                .order(2)
                .status(EXECUTED)
                .build();
        couchMove.executeMigration(newArrayList(changeLogToSkip, executedChangeLog));
        verify(dbServiceMock).save(changeLogToSkip);
        Assert.assertEquals(SKIPPED, changeLogToSkip.getStatus());
    }

    @Test
    public void should_execute_migrations() {
        CouchMove couchMove = spy(CouchMove.class);
        ChangeLog executedChangeLog = ChangeLog.builder()
                .version("1")
                .order(1)
                .status(EXECUTED)
                .build();
        executedChangeLog.setCas(RANDOM.nextLong());
        ChangeLog changeLog = ChangeLog.builder()
                .version("2")
                .type(Type.DOCUMENTS)
                .build();
        doReturn(true).when(couchMove).executeMigration(changeLog);
        couchMove.executeMigration(newArrayList(newArrayList(executedChangeLog, changeLog)));
        Assert.assertEquals((Integer) 2, changeLog.getOrder());
    }

    @Test(expected = CouchMoveException.class)
    public void should_throw_exception_if_migration_failed() {
        CouchMove couchMove = spy(CouchMove.class);
        ChangeLog changeLog = ChangeLog.builder()
                .version("1")
                .type(Type.N1QL)
                .build();
        doReturn(false).when(couchMove).executeMigration(changeLog);
        couchMove.executeMigration(newArrayList(changeLog));
    }

    @Test
    public void should_update_changeLog_on_migration_success() {
        ChangeLog changeLog = ChangeLog.builder()
                .version("1")
                .type(Type.DESIGN_DOC)
                .build();
        when(dbServiceMock.importDesignDoc(any())).thenReturn(true);
        couchMove.executeMigration(changeLog);
        verify(dbServiceMock).save(changeLog);
        Assert.assertNotNull(changeLog.getTimestamp());
        Assert.assertNotNull(changeLog.getDuration());
        Assert.assertNotNull(changeLog.getRunner());
        Assert.assertEquals(changeLog.getStatus(), Status.EXECUTED);
    }

    @Test
    public void should_update_changeLog_on_migration_failure() {
        ChangeLog changeLog = ChangeLog.builder()
                .version("1")
                .type(Type.DOCUMENTS)
                .build();
        when(dbServiceMock.importDocuments(any())).thenReturn(false);
        couchMove.executeMigration(changeLog);
        verify(dbServiceMock).save(changeLog);
        Assert.assertNotNull(changeLog.getTimestamp());
        Assert.assertNotNull(changeLog.getDuration());
        Assert.assertNotNull(changeLog.getRunner());
        Assert.assertEquals(changeLog.getStatus(), Status.FAILED);
    }

    private static Bucket mockBucket() {
        Bucket mockedBucket = mock(Bucket.class);
        when(mockedBucket.name()).thenReturn("default");
        return mockedBucket;
    }

}