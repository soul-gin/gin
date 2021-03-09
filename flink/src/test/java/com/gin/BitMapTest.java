package com.gin;

import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class BitMapTest {

    @Test
    public void roaringBitMap() {
        //向rr中添加1、2、3、1000四个数字
        RoaringBitmap rr = RoaringBitmap.bitmapOf(1, 2, 3, 1000);
        //创建RoaringBitmap rr2
        RoaringBitmap rr2 = new RoaringBitmap();
        //向rr2中添加10000-12000共2000个数字
        rr2.add(10000L, 12000L);
        //返回第3个数字是1000，第0个数字是1，第1个数字是2，则第3个数字是1000
        System.out.println(rr.select(3));
        System.out.println("---------");
        //返回value = 2 时的索引为 1。value = 1 时，索引是 0 ，value=3的索引为2
        System.out.println(rr.rank(2));
        System.out.println("---------");
        //判断是否包含1000
        System.out.println(rr.contains(1000));
        System.out.println("---------");
        //判断是否包含7 // will return false
        System.out.println(rr.contains(7));
        System.out.println("---------");

        //两个RoaringBitmap进行or操作，数值进行合并，合并后产生新的RoaringBitmap叫rror
        RoaringBitmap rror = RoaringBitmap.or(rr, rr2);
        //rr与rr2进行位运算，并将值赋值给rr
        rr.or(rr2);
        //判断rror与rr是否相等，显然是相等的
        boolean equals = rror.equals(rr);
        if (!equals) {
            throw new RuntimeException("bug");
        }
        // 查看rr中存储了多少个值，1,2,3,1000和10000-12000，共2004个数字
        long cardinality = rr.getLongCardinality();
        System.out.println(cardinality);
        System.out.println("---------");
        //遍历rr中的value
        for (int i : rr) {
            System.out.println(i);
        }
        System.out.println("---------");
    }

    @Test
    public void roaring64NavigableMap(){
        Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();
        roaring64NavigableMap.add(0L);
        roaring64NavigableMap.add(1L);
        roaring64NavigableMap.add(1000L);
        roaring64NavigableMap.add(9223372036854775806L);
        roaring64NavigableMap.add(9223372036854775807L);
        System.out.println(roaring64NavigableMap.contains(1000L));

    }


}