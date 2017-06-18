package com.github.couchmove.exception;

/**
 * @author ctayeb
 * Created on 28/05/2017
 */
public class CouchmoveException extends RuntimeException {
    public CouchmoveException(String message, Throwable cause) {
        super(message, cause);
    }

    public CouchmoveException(String message) {
        super(message);
    }
}
