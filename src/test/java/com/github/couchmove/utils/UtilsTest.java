package com.github.couchmove.utils;

import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.*;

/**
 * @author ctayeb
 * created on 18/06/2017
 */
public class UtilsTest {

    public static Stream<Arguments> durationSource() {
        return Stream.of(
                Arguments.of((((3 * 24) + 5) * 60) + 15, MINUTES, "3d 5h 15min"),
                Arguments.of(((5 * 60) * 60 + 12), SECONDS, "5h 12s"),
                Arguments.of((((((2 * 24) + 4) * 60) + 15) * 60) * 1000 + 312, MILLISECONDS, "2d 4h 15min 312ms"),
                Arguments.of(25_377_004_023L, MICROSECONDS, "7h 2min 57s 4ms 23μs"),
                Arguments.of(7_380_011_024_014L, NANOSECONDS, "2h 3min 11ms 24μs 14ns")
        );
    }

    @ParameterizedTest
    @MethodSource("durationSource")
    public void should_pretty_format_duration(long duration, TimeUnit source, String expectedFormat) {
        Assert.assertEquals(expectedFormat, Utils.prettyFormatDuration(duration, source));
    }

}
