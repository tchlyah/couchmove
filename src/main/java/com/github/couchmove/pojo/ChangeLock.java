package com.github.couchmove.pojo;

import lombok.Data;

import java.util.Date;

/**
 * Created by tayebchlyah on 27/05/2017.
 */
@Data
public class ChangeLock extends CouchbaseEntity {

    private boolean locked;

    private String uuid;

    private String runner;

    private Date timestamp;
}
