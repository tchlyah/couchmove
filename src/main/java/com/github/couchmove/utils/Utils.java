package com.github.couchmove.utils;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author ctayeb
 * Created on 04/06/2017
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
                return method.invoke(o).toString();
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                logger.error("Unable to get actual user name", e);
            }
        }
        return "unknown";
    }
}
