package com.github.couchmove.pojo;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.StrictAssertions.assertThat;

public class ChangeLogTest {

    @SuppressWarnings("unused")
    private static Stream<Arguments> compareTo() {
        return Stream.of(
                Arguments.of(null, null, 0),
                Arguments.of(null, "0", -1),
                Arguments.of("0", null, 1),
                Arguments.of("0", "0", 0),
                Arguments.of("0", "0.1", -1),
                Arguments.of("0.9", "0.11", -1),
                Arguments.of("0.11", "0.9", 1),
                Arguments.of("0.9", "0.9", 0),
                Arguments.of("20200203153040", "20200203153041", -1)
        );
    }

    @ParameterizedTest
    @MethodSource
    public void compareTo(String v1, String v2, int expected) {
        assertThat(ChangeLog.builder().version(v1).build().compareTo(ChangeLog.builder().version(v2).build())).isEqualTo(expected);
    }
}
