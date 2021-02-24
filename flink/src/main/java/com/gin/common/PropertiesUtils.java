package com.gin.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author gin
 * @date 2021/2/22
 */
public class PropertiesUtils {

    private static Properties prop = new Properties();

    static {
        try {
            InputStream inputStream = PropertiesUtils.class.getClassLoader().getResourceAsStream("flink-conf.properties");
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProp(String name){
         return prop.getProperty(name);
    }

    public static void main(String[] args) {
        System.out.println(PropertiesUtils.getProp("flink.streaming.app.name"));
    }

}
