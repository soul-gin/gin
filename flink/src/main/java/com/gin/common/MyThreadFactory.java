package com.gin.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
* @author gin
* @date 2021/2/22
*/
public class MyThreadFactory implements ThreadFactory {
    private final String namePrefix;
    private final AtomicInteger nextId = new AtomicInteger(1);


    public MyThreadFactory(String groupName) {
        // 定义线程组名称，在 jstack 问题排查时，非常有帮助
        namePrefix = "From MyThreadFactory's " + groupName + "-Worker-";
    }

    @Override
    public Thread newThread(Runnable task) {
        String name = namePrefix + nextId.getAndIncrement();
        Thread thread = new Thread(null, task, name, 0);
        System.out.println(thread.getName());
        return thread;
    }
}