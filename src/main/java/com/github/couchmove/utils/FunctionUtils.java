package com.github.couchmove.utils;

import java.util.function.Function;
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

    /**
     * Wrap a function throwing checked exceptions into {@link Function}
     *
     * @param function Function throwing Exceptions
     * @param <T>      the type of the input to the function
     * @param <R>      the type of the result of the function
     * @return Wrapped Function
     */
    public static <T, R> Function<T, R> unchecked(CheckedFunction<T, R> function) {
        return t -> {
            try {
                return function.apply(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public interface CheckedFunction<T, R> {
        R apply(T t) throws Exception;
    }
}
