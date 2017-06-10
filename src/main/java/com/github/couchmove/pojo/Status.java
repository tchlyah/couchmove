package com.github.couchmove.pojo;

/**
 * Describes the current status of a {@link ChangeLog} execution
 *
 * @author ctayeb
 * Created on 03/06/2017
 */
public enum Status {

    /**
     * The {@link ChangeLog} was successfully executed
     */
    EXECUTED,

    /**
     * The {@link ChangeLog} execution has failed
     */
    FAILED,

    /**
     * The {@link ChangeLog} execution was ignored
     */
    SKIPPED
}
