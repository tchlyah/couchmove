package com.github.couchmove.utils;

import java.util.function.Predicate;

/**
 * Created by tayebchlyah on 05/06/2017.
 */
public class FunctionUtils {
    public static <T> Predicate<T> not(Predicate<T> predicate) {
        return predicate.negate();
    }
}
