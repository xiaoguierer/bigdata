package com.bigdata.es.laosiji.estools;

import java.util.Map;

public class javabeantools {

    /**
     * java --> map
     * @param obj
     * @return
     */
    public static Map<?, ?> objectToMap(Object obj) {
        if(obj == null)
            return null;

        return new org.apache.commons.beanutils.BeanMap(obj);
    }
}
