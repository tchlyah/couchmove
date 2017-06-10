package com.github.couchmove.exception;

/**
 * @author ctayeb
 * Created on 28/05/2017
 */
public class CouchMoveException extends RuntimeException {
    public CouchMoveException(String message, Throwable cause) {
        super(message, cause);
    }

    public CouchMoveException(String message) {
        super(message);
    }
}
