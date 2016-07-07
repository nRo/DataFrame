package de.unknownreality.dataframe.common.mapping;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created by Alex on 08.03.2016.
 */

/**
 * Annotation that defines a mapped column.
 * If a header is defined, the right value is found by using the header name in a data container row.
 * If an index is defined, the value is found by using the value at this index from each data container row.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface MappedColumn {
    String header() default "";

    int index() default -1;
}
