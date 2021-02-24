package com.gin

import java.util
import java.util.Date

object Lv03_Functions {


  def main(args: Array[String]): Unit = {

    //  方法  函数
    println("-------1.函数参数----------")

    javaVariable()
    memberVariable(33)

    println("-------2.递归函数----------")

    //递归先写触底！  触发什么报错呀
    val i: Int = fun04(4)
    println(i)

    println("-------3.默认值函数----------")

    //同时传入 parm1 parm2
    defaultFunction(9, "def")
    //仅传入 parm1
    defaultFunction(22)
    //仅传入 parm1
    defaultFunction(parm2 = "自定义参数2")

    println("-------4.匿名函数----------")
    //函数是第一类值
    //签名(定义变量类型): (Int, Int) => Int  参数类型 => 返回值类型
    var yy: (Int, Int) => Int =
    //匿名函数： (a:Int,b:Int) => { a+b }  ：（参数实现列表）=> 函数体
    (a: Int, b: Int) => {
      a + b
    }
    println(yy(3, 4))


    println("--------5.嵌套函数---------")

    //执行外部函数
    nestedFunction("hello Nested function")

    println("--------6.偏应用函数---------")

    //日志打印功能
    //主方法调用
    applicationFunction(new Date(), "info", "ok")
    //偏应用函数
    info(new Date, "ok")
    error(new Date, "error...")

    println("--------7.可变参数---------")

    //可变参数函数, 类型需要保持一致
    variableParamsFunc(2)
    println()
    variableParamsFunc(1, 2, 3)

    println("--------8.高阶函数---------")

    //传入加法计算函数(函数也是值(对象))
    computer(3, 8, (x: Int, y: Int) => {
      x + y
    })
    //语法糖: 第一个下划线代表第一个参数, 第二个下划线代表第二个参数
    computer(3, 8, _ + _)
    println()

    computer(3, 8, (x: Int, y: Int) => {
      x * y
    })
    computer(3, 8, _ * _)
    println()

    //传入函数工厂, 工厂翻译符号(策略)
    computer(3, 8, computerFactory("-"))


    println("--------9.柯里化---------")

    curryingNotFunc(3)(8)("str1")("str2")
    println()
    curryingFunc(1, 2, 3)("str1", "str2")


    println("--------*.方法---------")
    println("-------memberMethod----------")
    //方法不想执行，赋值给一个引用  方法名+空格+下划线
    val funcNotExec = memberMethod _
    funcNotExec()

    //赋值就会执行
    val funcExecWhenDef: Unit = memberMethod()


    //语法 ->  编译器  ->  字节码   <-  jvm规则
    //编译器，衔接 人  机器
    //java 中 + : 关键字
    //scala中 + : 方法/函数
    //scala语法中, 没有基本类型, 所以你写一个数字:3, 编辑器/语法，其实是把 3 看待成Int这个对象
    println()
    println(3 + 2)
    println(3.+(2))
    println(3:Int)


  }



  //成员方法 自动推断返回值类型(根据最后一行语句结果类型)
  //返回 Int
  def javaVariable(): util.LinkedList[String] = {
    //注意: 有return则必须要声明返回类型
    //scala 和 java可以混编, 最终执行在jvm, 遵循jvm规范即可
    return new util.LinkedList[String]()
  }

  //Unit (类似void, 无返回)
  //参数需要指定类型
  def memberVariable(x: Int) = {
    println(s"hello memberMethod $x")
  }

  //递归先写触底
  def fun04(num: Int): Int = {
    if (num == 1) {
      //返回值
      num
    } else {
      //递归调用
      num * fun04(num - 1)
    }
  }

  def defaultFunction(parm1: Int = 8, parm2: String = "parm2"): Unit = {
    println(s"$parm1\t$parm2")
  }

  def nestedFunction(str: String): Unit = {
    //嵌套函数
    def fun05(): Unit = {
      println(str)
    }
    //调用内部嵌套的函数, 不会调用上面的同名全局函数
    //内部调用优先级更高
    fun05()
  }

  //主方法
  def applicationFunction(date: Date, logType: String, msg: String): Unit = {
    //类似日志打印
    println(s"$date\t$logType\t$msg")
  }

  //派生方法(日志类型: 固定值(info,error);  传递给主方法的值 _: )
  //不指定, 使用推断类型
  var info = applicationFunction(_: Date, "info", _: String)
  //指定函数类型
  var error: (Date, String) => Unit = applicationFunction(_: Date, "error", _: String)

  //seqs 收集可变参数集合
  def variableParamsFunc(seqs: Int*): Unit = {
    //for遍历
    /*for (e <- seqs) {
      println(e)
    }*/


    //def foreach[U](f: A => U): Unit
    //f: A => U 表示入参为函数; Unit表示返回值为空
    //传入匿名函数
    seqs.foreach((x: Int) => {
      println(x + 1)
    })

    //传入函数
    //seqs.foreach(println)
  }

  //函数作为参数，函数作为返回值
  //函数作为参数
  def computer(a: Int, b: Int, f: (Int, Int) => Int): Unit = {
    val res: Int = f(a, b)
    println(res)
  }

  //函数作为返回值
  //参数是: (i: String)
  //返回值是函数类型(参数:两个Int, 返回值:Int): (Int, Int) => Int
  def computerFactory(i: String): (Int, Int) => Int = {
    //加号使用的函数(符号翻译)
    def plus(x: Int, y: Int): Int = {
      x + y
    }

    if (i.equals("+")) {
      plus
    } else {
          //非加号处理方式, 匿名函数
      (x: Int, y: Int) => {
        x - y
      }
    }
  }

  //固定参数列表, 未柯里化
  def curryingNotFunc(a: Int)(b: Int)(c: String)(d: String): Unit = {
    println(s"$a\t$b\t$c\t$d")
  }

  //可变参数列表, 柯里化
  def curryingFunc(a: Int*)(b: String*): Unit = {
    a.foreach(println(_))
    println()
    b.foreach(println)
  }

  //成员方法
  def memberMethod(): Unit = {
    println("hello memberMethod")
  }

  /*
  学习scala就是为了多学一门语言吧？
  感觉不如python，不仅学了语言，也学了工具。
  理解有哪些偏差？ 老师？？

  编译型  C   《   贼快
  解释型  python   《   慢  贼慢

  JAVA：其实不值钱，最值钱的是JVM

  JAVA：  解释型，编译过程，类型   比 python 快
  JVM：为什么值钱  是C写的， 【字节码（二进制） >JVM（堆/堆外（二进制））<  kernel（mmap，sendfile） 】  更快！！

   */


}
