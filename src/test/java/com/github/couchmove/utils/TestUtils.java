package com.github.couchmove.utils;

import com.github.couchmove.pojo.ChangeLog;
import com.github.couchmove.pojo.Status;
import com.github.couchmove.pojo.Type;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

/**
 * @author ctayeb
 * Created on 01/06/2017
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
                .build();
    }

    @SafeVarargs
    public static void assertThrows(Runnable runnable, Class<? extends Throwable>... throwables) {
        boolean exceptionOccurred = false;
        try {
            runnable.run();
            // Then an exception should raise
        } catch (Exception e) {
            Assertions.assertThat(e).isOfAnyClassIn(throwables);
            exceptionOccurred = true;
        }
        assertTrue("Expected exception occurred", exceptionOccurred);
    }
}
