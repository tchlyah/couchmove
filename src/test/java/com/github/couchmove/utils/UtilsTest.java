package com.github.couchmove.utils;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;

/**
 * @author ctayeb
 *         created on 18/06/2017
 */
@RunWith(DataProviderRunner.class)
public class UtilsTest {

    @DataProvider
    public static Object[][] durationProvider() {
        return new Object[][]{
                {(((3 * 24) + 5) * 60) + 15, MINUTES, "3d 5h 15min"},
                {((5 * 60) * 60 + 12), SECONDS, "5h 12s"},
                {(((((2 * 24) + 4) * 60) + 15) * 60) * 1000 + 312, MILLISECONDS, "2d 4h 15min 312ms"},
                {25_377_004_023L, MICROSECONDS, "7h 2min 57s 4ms 23μs"},
                {7_380_011_024_014L, NANOSECONDS, "2h 3min 11ms 24μs 14ns"}
        };
    }

    @Test
    @UseDataProvider("durationProvider")
    public void should_pretty_format_duration(long duration, TimeUnit source, String expectedFormat) {
        Assert.assertEquals(expectedFormat, Utils.prettyFormatDuration(duration, source));
    }

}