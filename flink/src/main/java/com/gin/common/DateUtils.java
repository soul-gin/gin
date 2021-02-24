package com.gin.common;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author gin
 * @date 2021/2/22
 */
public class DateUtils {

    public static String getMin(Date date){
        String pattern = "yyyyMMddHHmm";
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        return dateFormat.format(date);
    }

}
