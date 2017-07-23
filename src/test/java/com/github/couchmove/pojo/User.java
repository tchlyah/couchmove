package com.github.couchmove.pojo;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author ctayeb
 * Created on 07/06/2017
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class User extends CouchbaseEntity {
    private final String type;
    private final String username;
    private final String birthday;
}
