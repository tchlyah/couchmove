package com.github.couchmove.service;

import com.github.couchmove.exception.CouchMoveException;
import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.repository.CouchbaseRepository;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
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
import static com.github.couchmove.service.ChangeLogDBService.extractRequests;
import static com.github.couchmove.utils.TestUtils.*;
import static org.mockito.Mockito.when;

/**
 * @author ctayeb
 * Created on 03/06/2017
 */
@RunWith(MockitoJUnitRunner.class)
public class ChangeLogDBServiceTest {

    @InjectMocks
    private ChangeLogDBService service = new ChangeLogDBService(null);

    @Mock
    private CouchbaseRepository<ChangeLog> repository;

    @Before
    public void init() {
        when(repository.getBucketName()).thenReturn("default");
    }

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

    @Test
    public void should_fetch_return_unchanged_changeLogs() {
        // Given changeLogs stored in DB
        ChangeLog changeLog1 = getRandomChangeLog();
        changeLog1.setCas(RANDOM.nextLong());
        when(repository.findOne(PREFIX_ID + changeLog1.getVersion())).thenReturn(changeLog1);
        ChangeLog changeLog2 = getRandomChangeLog();
        changeLog2.setCas(RANDOM.nextLong());
        when(repository.findOne(PREFIX_ID + changeLog2.getVersion())).thenReturn(changeLog2);

        // When we call service with the later
        List<ChangeLog> changeLogs = Lists.newArrayList(changeLog1, changeLog2);
        List<ChangeLog> result = service.fetchAndCompare(changeLogs);

        // Then nothing should be returned
        Assert.assertEquals(changeLogs, result);
        Assert.assertNotNull(changeLogs.get(0).getCas());
        Assert.assertNotNull(changeLogs.get(1).getCas());
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
    public void should_return_updated_changeLog_checksum_with_cas_reset_if_checksum_reset() {
        // Given a changeLog stored on DB
        ChangeLog dbChangeLog = getRandomChangeLog();
        dbChangeLog.setChecksum(null);
        dbChangeLog.setCas(RANDOM.nextLong());
        when(repository.findOne(PREFIX_ID + dbChangeLog.getVersion())).thenReturn(dbChangeLog);

        // And a changeLog with different description
        ChangeLog changeLog = dbChangeLog.toBuilder()
                .checksum(getRandomString())
                .build();

        // When we call service with the later
        ArrayList<ChangeLog> changeLogs = Lists.newArrayList(changeLog);
        List<ChangeLog> result = service.fetchAndCompare(changeLogs);

        // Then it should be same
        Assert.assertEquals(changeLogs, result);
        Assert.assertNull(changeLog.getCas());
    }

    @Test
    public void should_return_updated_changeLog_with_cas_reset_if_description_changed() {
        // Given a changeLog stored on DB
        ChangeLog dbChangeLog = getRandomChangeLog();
        dbChangeLog.setCas(RANDOM.nextLong());
        when(repository.findOne(PREFIX_ID + dbChangeLog.getVersion())).thenReturn(dbChangeLog);

        // And a changeLog with different description
        ChangeLog changeLog = dbChangeLog.toBuilder()
                .description(getRandomString())
                .script(getRandomString())
                .build();

        // When we call service with the later
        ArrayList<ChangeLog> changeLogs = Lists.newArrayList(changeLog);
        List<ChangeLog> result = service.fetchAndCompare(changeLogs);

        // Then it should be same
        Assert.assertEquals(changeLogs, result);
        Assert.assertNull(changeLog.getCas());
    }

    @Test
    public void should_skip_n1ql_blank_and_comment_lines() {
        String request1 = "CREATE INDEX 'user_index' ON default\n" +
                "  WHERE type = 'user'";
        String request2 = "INSERT { 'name': 'toto'} INTO default";
        String sql = "-- create Index\n" +
                request1 + ";\n" +
                "\n" +
                "/*insert new users*/\n" +
                request2 + "; ";

        Assertions.assertThat(extractRequests(sql)).containsExactly(request1, request2);
    }
}