package com.gin

object Lv08_PartialFunction {


  def main(args: Array[String]): Unit = {

    println(myPartialFunction(44))
    println(myPartialFunction("hello"))
    println(myPartialFunction("hi"))

  }

  /**
    * PartialFunction 偏函数
    * Any 表示可以传入任何类型
    * String 表示输出为 字符串类型
    * @return
    */
  def myPartialFunction: PartialFunction[Any, String] = {
    case "hello" => "val is hello"
    case x: Int => s"$x...is int"
    case _ => "none"
  }


}
