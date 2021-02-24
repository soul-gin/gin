package com.gin

import java.util

object Lv09_Implicit {

  def main(args: Array[String]): Unit = {

    val linkedList = new util.LinkedList[Int]()
    linkedList.add(1)
    linkedList.add(2)

    val arrayList = new util.ArrayList[Int]()
    arrayList.add(1)
    arrayList.add(2)
    arrayList.add(3)


    //问题:
    //如何让java工具类也可以使用类scala语法,且类型通用
    //在这里是不能调用 foreach 方法的, 因为 list 中无对应方法
    //linkedList.foreach(println)
    //arrayList.foreach(println)


    //java处理方式:
    //包装类:
    // new MyList(linkedList);
    // MyList中定义foreach方法
    // MyList.foreach(myFunc);
    //即: 把对象和方法传递进去


    //scala处理方式:
    //implicit(隐式转换): 隐式转换方法
    /*
    这些代码最终交给的是scala的编译器！
    scala编译器发现 list.foreach(println)  有bug,linkedList没有foreach方法
    去寻找有没有 implicit 定义的方法，且方法的参数正好是LinkedList的类型
    编译期：完成你曾经人手写代码工作
    //val linkedList = new MyList(list)
    //linkedList.foreach(println)
    编译器帮你把代码改写了
    应用: spark的RDD, 通过scala的implicit来扩展类的方法,而不修改类本身
    */

    implicit def implicitLinkedList[T](list: util.LinkedList[T]): MyList[T] = {
      //val linkedList = new MyList(list)
      //linkedList.foreach(println)
      val iter: util.Iterator[T] = list.iterator()
      new MyList(iter)
    }

    implicit def implicitArrayList[T](list: java.util.ArrayList[T]): MyList[T] = {
      //完成代码
      val iter: util.Iterator[T] = list.iterator()
      new MyList(iter)
    }

    //这里不会报错, 虽然list没有foreach方法, 但是会向上寻找implicit方法
    //找到匹配到对应 LinkedList ArrayList 类型的方法进行解释
    linkedList.foreach(println)
    arrayList.foreach(println)

    //implicit 隐式转换同一个作用域中必须保障类型唯一
    implicit val defaultName: String = "gin-soul"

    //柯里化, age不需要隐式转换, name可以隐式转换
    def myPrint(age: Int)(implicit name: String): Unit = {
      println(name + " " + age)
    }

    myPrint(55)("zhang.san")
    //使用作用域中默认的String类型的隐式转换 gin-soul
    myPrint(66)

  }
}

class MyList[T](list: util.Iterator[T]) {

  //自定义 foreach 函数; 接收java的util.Iterator对象
  //使得 myList.foreach 进行迭代处理
  //注意: 只有一个参数时, (T) 可以省略为 T
  def foreach(func: T => Unit): Unit = {
    //迭代并交给 func 处理
    while (list.hasNext) func(list.next())
  }

}

