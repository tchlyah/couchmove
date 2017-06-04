package com.github.couchmove.pojo;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

/**
 * Created by tayebchlyah on 27/05/2017.
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class ChangeLock extends CouchbaseEntity {

    private boolean locked;

    private String uuid;

    private String runner;

    private Date timestamp;
}
