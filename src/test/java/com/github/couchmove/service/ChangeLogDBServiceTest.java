package com.github.couchmove.service;

import com.github.couchmove.exception.CouchMoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.repository.CouchbaseRepository;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import static com.github.couchmove.service.ChangeLogDBService.PREFIX_ID;
import static com.github.couchmove.utils.TestUtils.getRandomChangeLog;
import static com.github.couchmove.utils.TestUtils.getRandomString;
import static org.mockito.Mockito.when;

/**
 * Created by tayebchlyah on 03/06/2017.
 */
@RunWith(MockitoJUnitRunner.class)
public class ChangeLogDBServiceTest {

    @InjectMocks
    private ChangeLogDBService service = new ChangeLogDBService(null);

    @Mock
    private CouchbaseRepository<ChangeLog> repository;

    @Test
    public void should_fetch_return_same_changeLogs_when_absent() {
        // Given changeLogs stored in DB
        when(repository.findOne(Mockito.anyString())).thenReturn(null);

        // When we call service with the later
        List<ChangeLog> changeLogs = Lists.newArrayList(getRandomChangeLog(), getRandomChangeLog());
        List<ChangeLog> result = service.fetchAndCompare(changeLogs);

                // Then we should return the same
                Assert.assertEquals(changeLogs, result);
    }

    @Test(expected = CouchMoveException.class)
    public void should_fetch_fail_when_checksum_does_not_match() {
        // Given a changeLog stored on DB
        ChangeLog dbChangeLog = getRandomChangeLog();
        when(repository.findOne(PREFIX_ID + dbChangeLog.getVersion())).thenReturn(dbChangeLog);

        // And a changeLog with same version but different checksum
        ChangeLog changeLog = getRandomChangeLog();
        changeLog.setVersion(dbChangeLog.getVersion());
        changeLog.setChecksum(getRandomString());

        // When we call service with the later changeLog
        service.fetchAndCompare(Lists.newArrayList(changeLog));

        // Then an exception should rise
    }

    @Test
    public void should_return_updated_changeLog_if_info_changed() {
        // Given a changeLog stored on DB
        ChangeLog dbChangeLog = getRandomChangeLog();
        when(repository.findOne(PREFIX_ID + dbChangeLog.getVersion())).thenReturn(dbChangeLog);

        // And a changeLog with different description
        ChangeLog changeLog = ChangeLog.builder()
                .version(dbChangeLog.getVersion())
                .type(dbChangeLog.getType())
                .checksum(dbChangeLog.getChecksum())
                .runner(dbChangeLog.getRunner())
                .duration(dbChangeLog.getDuration())
                .order(dbChangeLog.getOrder())
                .status(dbChangeLog.getStatus())
                .description(getRandomString())
                .script(getRandomString())
                .build();
        changeLog.setVersion(dbChangeLog.getVersion());
        changeLog.setChecksum(dbChangeLog.getChecksum());

        // When we call service with the later
        ArrayList<ChangeLog> changeLogs = Lists.newArrayList(changeLog);
        List<ChangeLog> result = service.fetchAndCompare(changeLogs);

        // Then it should be same
        Assert.assertEquals(changeLogs, result);
    }
}