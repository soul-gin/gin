package com.gin

/**
  * match, 可以匹配多种类型
  * 与java中的switch类似
  */
object Lv07_Match {

  def main(args: Array[String]): Unit = {
    //定义一个多数据类型的元组
    val tuple: (Int, Double, String, Boolean, Int) = (99, 88.0, "str", false, 44)

    //获取迭代器
    val iter: Iterator[Any] = tuple.productIterator

    //map返回值依然是迭代器
    val res: Iterator[Unit] = iter.map(
      //map, 将元素都进行一次匹配, 且只会从上开始命中一条
      (x) => {
        x match {
          //比较值
          case 88 => println(s"$x ...is 88")
          case false => println(s"$x...is false")
          //判断类型且比较值
          case w: Int if w > 50 => println(s"$w...is int, > 50")
          //判断类型
          case o: Int => println(s"$o...$x...is int")
          // case _ 类似switch中的default方法, 未匹配到的默认处理方式
          case _ => println("I don't know what type it is")
        }
      }
    )

    //迭代器需要被使用才能调起处理数据
    //因为map需要返回值, 目前定义的是Unit, 所以会打印 () 表示Unit
    while (res.hasNext) println(res.next())

  }


}
