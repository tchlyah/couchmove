package com.github.couchmove.pojo;

import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;

public class ChangeLogTest {

    @Test
    public void should_compare_version_properly() {

        ChangeLog changeLog1 = ChangeLog.builder().version("0.9").build();
        ChangeLog changeLog2 = ChangeLog.builder().version("0.11").build();
        ChangeLog changeLog3 = ChangeLog.builder().version("0.9").build();

        assertEquals(-1, changeLog1.compareTo(changeLog2));
        assertEquals(1, changeLog2.compareTo(changeLog1));
        assertEquals(0, changeLog1.compareTo(changeLog3));
    }
}
