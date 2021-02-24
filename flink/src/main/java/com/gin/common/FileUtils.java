package com.gin.common;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

/**
 * @author gin
 * @date 2021/2/20
 */
public class FileUtils {

    public static List<String> toListByRandomAccessFile(String name) {
        // 使用 LinkedList 来存储每行读取到的字符串
        LinkedList<String> list = new LinkedList<>();
        try {
            File file = new File(name);
            RandomAccessFile fileR = new RandomAccessFile(file, "r");

            // 按行读取字符串
            String str = null;
            while ((str = fileR.readLine()) != null) {
                list.add(new String(str.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
            }
            fileR.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return list;
    }

    public static boolean clearDir(String... filePaths) {
        boolean flag = false;
        for (String sPath : filePaths) {

            File file = new File(sPath);
            // 判断目录或文件是否存在
            if (file.exists()) {
                // 判断是否为文件
                // 为文件时调用删除文件方法
                if (file.isFile()) {
                    flag = file.delete();
                } else {  // 为目录时调用删除目录方法
                    flag = deleteDirectory(sPath);
                }
            }
        }
        return flag;
    }

    private static boolean deleteFile(String sPath) {
        boolean flag = false;
        File file = new File(sPath);
        // 路径为文件且不为空则进行删除
        if (file.isFile() && file.exists()) {
            flag = file.delete();
        }
        return flag;
    }

    private static boolean deleteDirectory(String sPath) {
        //如果sPath不以文件分隔符结尾，自动添加文件分隔符
        if (!sPath.endsWith(File.separator)) {
            sPath = sPath + File.separator;
        }
        File dirFile = new File(sPath);
        //如果dir对应的文件不存在，或者不是一个目录，则退出
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            return false;
        }
        boolean flag = true;
        //删除文件夹下的所有文件(包括子目录)
        File[] files = dirFile.listFiles();
        if (files != null) {
            for (File file : files) {
                //删除子文件
                if (file.isFile()) {
                    flag = deleteFile(file.getAbsolutePath());
                    if (!flag) {
                        break;
                    }
                } //删除子目录
                else {
                    flag = deleteDirectory(file.getAbsolutePath());
                    if (!flag) {
                        break;
                    }
                }
            }
        }
        if (!flag) {
            return false;
        }
        //删除当前目录
        return dirFile.delete();
    }

}
