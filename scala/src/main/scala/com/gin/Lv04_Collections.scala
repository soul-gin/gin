package com.gin

import java.util

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Lv04_Collections {


  def main(args: Array[String]): Unit = {

    //java coder
    val listJava = new util.LinkedList[String]()
    listJava.add("hello")
    //scala
    scalaArray

    println("--------------list-------------")
    //2.链表
    //scala中collections中有个2个包：immutable，mutable  默认的是不可变的immutable
    scalaList

    println("--------------Set-------------")
    scalaSet

    println("--------------tuple-------------")
    //元组
    scalaTuple()


    println("--------------map-------------")
    scalaMap

    println("--------------艺术-------------")
    scalaArt1

    println("--------------艺术-升华------------")
    scalaArt2

    println("--------------艺术-再-升华------------")
    // 迭代器模式, 不存数据存指针, 传递计算函数
    // 方法没有被执行, 返回了一个迭代器对象, 也就不存在存储中间状态占用内存的问题
    // (计算向数据移动)
    scalaArt3()


  }


  private def scalaArt3() = {
    //迭代器(iterator): 解决数据计算中间状态占用内存这一问题
    //基于迭代器的原码分析(迭代器不存数据, 仅存指针!!! 就不存在数据冗余)
    val listStr = List(
      "hello world",
      "hello msb",
      "good idea"
    )

    // 什么是迭代器，为什么会有迭代器模式(节省存储多份数据的空间)
    // 迭代器不存数据, 仅存指针
    // 迭代器里不存数据！
    val iter: Iterator[String] = listStr.iterator

    //扁平化
    val iterFlatMap = iter.flatMap((x: String) => x.split(" "))
    //注意: 这里不能迭代打印, 避免后续在iterFlatMap.map时迭代的指针到了末尾, 不再继续迭代
    //iterFlatMap.foreach(println)
    //映射
    val iterMapList = iterFlatMap.map((_, 1))

    //1.listStr  真正的数据集，有数据的
    //2.iter.flatMap  没有发生计算，返回了一个新的迭代器
    while (iterMapList.hasNext) {
      val tuple: (String, Int) = iterMapList.next()
      println(tuple)
    }

  }

  private def scalaArt2 = {
    val listStr = List(
      "hello world",
      "hello gin",
      "good idea"
    )
    /*
    //支持 array
    val listStr = Array(
      "hello world",
      "hello msb",
      "good idea"
    )

    //支持set
    val listStr = Set(
      "hello world",
      "hello msb",
      "good idea"
    )
    */

    // 代码存在问题
    // 内存扩大了N倍，每一步计算内存都留有对象数据(listStr  flatMap  mapList)
    // 现成的技术解决数据计算中间状态占用内存这一问题 -> 迭代器(iterator)
    //flatMap 扁平化
    //x.split(" ") 按空格切割
    val flatMap = listStr.flatMap((x: String) => x.split(" "))
    flatMap.foreach((elem: String) => {
      print(s"$elem ")
    })
    println()
    //map 映射
    val mapList = flatMap.map((_, 1))
    mapList.foreach(println)

  }

  private def scalaArt1 = {
    val list = List(1, 2, 3, 4, 5, 6)

    println("--------元素+10--------")
    list.foreach((elem: Int) => {
      print(s"$elem ")
    })
    println()

    //元素均加10
    val listMap: List[Int] = list.map((x: Int) => x + 10)
    listMap.foreach((elem: Int) => {
      print(s"$elem ")
    })
    println()

    //元素放大10倍
    val listMap02: List[Int] = list.map(_ * 10)
    println("--------元素*10--------")
    list.foreach((elem: Int) => {
      print(s"$elem ")
    })
    println()
    listMap02.foreach((elem: Int) => {
      print(s"$elem ")
    })
    println()
  }

  private def scalaMap = {
    //immutable 不可变
    //两种表示键值对方式:
    // ("a", 33)
    // "b" -> 22
    val map01: Map[String, Int] = Map(("a", 33), "b" -> 22, ("c", 3434), ("a", 3333))
    val keys: Iterable[String] = map01.keys

    //option:
    // none: 可以用默认值替换
    // some: 有值则直接取出
    //println(map01.get("a").getOrElse("hello world"))
    //简写
    println(map01.getOrElse("a", "hello world"))
    println(map01.get("w").getOrElse("hello world"))

    //遍历方式1
    for (elem <- keys) {
      println(s"key: $elem   value: ${map01.get(elem).get}")
    }

    //遍历方式2
    //keys.foreach()

    //定义可变map
    val map02: mutable.Map[String, Int] = scala.collection.mutable.Map(("a", 11), ("b", 22))
    map02.put("c", 22)
  }

  private def scalaTuple(): Unit = {
    //new 可以省略不写
    //val t2 = new Tuple2(11,"sdfsdf")
    //两个元素的Tuple2; 在scala描绘的是K,V
    val t2 = (11, "parms2")
    //三个元素Tuple3; (数字, 字符串, 字符)
    val t3 = Tuple3(22, "parms2", '3')
    //四个元素Tuple4;
    val t4: (Int, Int, Int, Int) = (1, 2, 3, 4)
    //22个元素Tuple22; 最多22个(上限)
    //定义了一个在元组第1位处输入两个参数, 返回一个22位的元组的函数
    val t22: ((Int, Int) =>
      Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    = ((a: Int, b: Int) =>
      a + b + 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4)

    //取t2的第1个元素
    println(t2._1)
    //取t4的第3个元素
    println(t4._3)
    //获取元组第1位, 执行函数
    println(t22._1(1, 2))
    //获取元组第二位
    println(t22._2)

    //遍历元组
    val tIter: Iterator[Any] = t22.productIterator
    while (tIter.hasNext) {
      //第一位为函数, 其余为数字
      print(tIter.next() + " ")
    }
    println()
  }

  private def scalaSet = {
    //不可变, 去重
    val set01: Set[Int] = Set(1, 2, 3, 3, 3, 2, 1)
    for (elem <- set01) {
      print(s"$elem ")
    }
    println()
    //set01.foreach(println)

    //可变的,去重(需要指定 mutable.Set)
    val set02: mutable.Set[Int] = mutable.Set(11, 22, 22, 22, 11)
    set02.add(33)
    set02.foreach((elem: Int) => {
      print(s"$elem ")
    })
    println()

    //或全限定名指定
    val set03: Predef.Set[Int] = scala.collection.immutable.Set(33, 44, 22, 11)
    set03.foreach((elem: Int) => {
      print(s"$elem ")
    })
    println()
  }

  private def scalaArray = {
    println("--------------array-------------")
    //scala还有自己的collections
    //1.数组
    //Java中泛型是<>  scala中泛型是[]，所以数组用 (n)
    //  val 约等于 final  不可变描述的是val指定的引用的值（值：字面值，地址）
    val arr01 = Array[Int](1, 2, 3)
    //可以改元素值, 但是不能改引用的值
    //arr01=Array(1,2,3,3,3,3)
    arr01(1) = 99
    println(arr01(0))

    for (elem <- arr01) {
      print(s"$elem ")
    }
    println()
    //遍历元素，需要函数接收元素
    arr01.foreach((elem: Int) => {
      print(s"$elem ")
    })
    println()

  }

  def scalaList: Unit = {
    //immutable  不可变list
    val list01 = List(1, 2, 3, 2, 1)
    //遍历方式1
    for (elem <- list01) {
      print(s"$elem ")
    }
    println()

    //遍历方式2
    //list01.foreach(println)

    //不可变, 不能 += (新增扩容)
    //list01.+=(4)

    //mutable  可变list
    val list02 = new ListBuffer[Int]()
    list02.+=(1)
    list02.+=(2)
    list02.+=(3)

    //scala数据集中的  ++ +=  ++：  ：++
    list02.foreach((elem: Int) => {
      print(s"$elem ")
    })
    println()
  }
}
