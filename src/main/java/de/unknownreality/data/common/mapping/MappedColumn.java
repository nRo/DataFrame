package de.unknownreality.data.common.mapping;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created by Alex on 08.03.2016.
 */

@Retention(RetentionPolicy.RUNTIME)
public @interface MappedColumn {
    String header() default "";
    int index() default -1;
}
