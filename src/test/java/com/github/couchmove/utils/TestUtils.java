package com.github.couchmove.utils;

import org.jetbrains.annotations.NotNull;

import java.util.UUID;

import static org.apache.commons.io.IOUtils.toByteArray;

/**
 * Created by tayebchlyah on 01/06/2017.
 */
public class TestUtils {

    @NotNull
    public static String getRandomString() {
        return UUID.randomUUID().toString();
    }

}
