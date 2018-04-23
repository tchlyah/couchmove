package com.github.couchmove.utils;

import com.google.common.base.Stopwatch;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.*;

/**
 * @author ctayeb
 *         Created on 04/06/2017
 */
public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    /**
     * Get the username for the current OS user
     */
    @Getter(lazy = true)
    private static final String username = initializeUserName();

    private static String initializeUserName() {
        String osName = System.getProperty("os.name").toLowerCase();
        String className = null;
        String methodName = "getUsername";

        if (osName.contains("windows")) {
            className = "com.sun.security.auth.module.NTSystem";
            methodName = "getName";
        } else if (osName.contains("linux") || osName.contains("mac")) {
            className = "com.sun.security.auth.module.UnixSystem";
        } else if (osName.contains("solaris") || osName.contains("sunos")) {
            className = "com.sun.security.auth.module.SolarisSystem";
        }

        if (className != null) {
            try {
                Class<?> c = Class.forName(className);
                Method method = c.getDeclaredMethod(methodName);
                Object o = c.newInstance();
                Object name = method.invoke(o);
                if (name != null) {
                    return name.toString();
                }
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                logger.error("Unable to get actual user name", e);
            }
        }
        return "unknown";
    }

    /**
     * Format duration to human readable
     * <p>
     * Exemple : 188,100,312 {@link TimeUnit#MILLISECONDS} → 2d 4h 15m 15s 312ms
     *
     * @param duration duration to format
     * @param timeUnit source timeUnit
     * @return human readable duration format
     */
    public static String prettyFormatDuration(long duration, TimeUnit timeUnit) {
        StringBuffer sb = new StringBuffer();
        duration = appendUnit(sb, duration, timeUnit, DAYS/*    */, "d", timeUnit::toDays);
        duration = appendUnit(sb, duration, timeUnit, HOURS/*   */, "h", timeUnit::toHours);
        duration = appendUnit(sb, duration, timeUnit, MINUTES/* */, "min", timeUnit::toMinutes);
        duration = appendUnit(sb, duration, timeUnit, SECONDS/* */, "s", timeUnit::toSeconds);
        duration = appendUnit(sb, duration, timeUnit, MILLISECONDS, "ms", timeUnit::toMillis);
        duration = appendUnit(sb, duration, timeUnit, MICROSECONDS, "μs", timeUnit::toMicros);
        /*      */
        appendUnit(sb, duration, timeUnit, NANOSECONDS, "ns", timeUnit::toNanos);
        return sb.toString();
    }

    private static long appendUnit(StringBuffer sb, long duration, TimeUnit source, TimeUnit destination, String unit, Function<Long, Long> converter) {
        long value = converter.apply(duration);
        if (value != 0) {
            sb.append(value).append(unit);
            long remaining = duration - source.convert(value, destination);
            if (remaining != 0) {
                sb.append(" ");
            }
            return remaining;
        }
        return duration;
    }

    public static String elapsed(Stopwatch sw) {
        return prettyFormatDuration(sw.elapsed(TimeUnit.MILLISECONDS), MILLISECONDS);
    }
}
