package com.github.couchmove.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

/**
 * @author ctayeb
 * Created on 07/06/2017
 */
@EqualsAndHashCode(callSuper = false)
@Data
@AllArgsConstructor
@NoArgsConstructor(force = true, access = PRIVATE)
public class User extends CouchbaseEntity {
    private final String type;
    private final String username;
    private final String birthday;
}
