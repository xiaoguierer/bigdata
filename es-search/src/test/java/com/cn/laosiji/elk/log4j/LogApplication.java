package com.cn.laosiji.elk.log4j;

import org.apache.log4j.Logger;

public class LogApplication {
    private static final Logger LOGGER = Logger.getLogger(LogApplication.class);
    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 100; i++) {
            LOGGER.info("now is --- "+i);
            LOGGER.error("Info log [" + i + "].");
            Thread.sleep(500);
        }
    }
}
