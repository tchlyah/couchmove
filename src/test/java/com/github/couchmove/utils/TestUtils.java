package com.github.couchmove.utils;

import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.pojo.Status;
import com.github.couchmove.pojo.Type;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.UUID;

/**
 * Created by tayebchlyah on 01/06/2017.
 */
public class TestUtils {

    public static final Random RANDOM = new Random();

    @NotNull
    public static String getRandomString() {
        return UUID.randomUUID().toString();
    }

    @NotNull
    public static ChangeLog getRandomChangeLog() {
        Type type = Type.values()[Math.abs(RANDOM.nextInt(Type.values().length))];
        String version = getRandomString();
        String description = getRandomString().replace("-", "_");
        return ChangeLog.builder()
                .version(version)
                .description(description)
                .type(type)
                .script("V" + version + "__" + description + (!type.getExtension().isEmpty() ? "." + type.getExtension() : ""))
                .duration(RANDOM.nextLong())
                .checksum(getRandomString())
                .status(Status.values()[Math.abs(RANDOM.nextInt(Status.values().length))])
                .build();
    }
}
