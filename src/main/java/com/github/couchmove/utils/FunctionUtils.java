package com.github.couchmove.utils;

import java.util.function.Predicate;

/**
 * @author ctayeb
 * Created on 05/06/2017
 */
public class FunctionUtils {
    /**
     * Returns a {@link Predicate} that represents the logical negation of this
     * predicate.
     *
     * @param predicate {@link Predicate} to negate
     * @param <T>       the type of the input to the predicate
     * @return a predicate that represents the logical negation of this
     * predicate
     */
    public static <T> Predicate<T> not(Predicate<T> predicate) {
        return predicate.negate();
    }
}
