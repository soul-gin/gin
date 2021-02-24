package com.gin


object Lv05_Trait {

  def main(args: Array[String]): Unit = {

    val p = new Person("zhangsan")
    p.hello()
    p.say()
    p.cry()
    p.hurt()

  }
}

/**
  * trait 可以参照java的interface(接口)和abstract(抽象类)
  * 可以认为是两者的结合
  */
trait God {
  def say(): Unit = {
    println("God...say")
  }
}

trait Devil {
  def cry(): Unit = {
    println("Devil...cry")
  }

  //接口
  def hurt(): Unit
}

//语法糖多继承, 实际还是基于jvm
class Person(name: String) extends God with Devil {

  def hello(): Unit = {
    println(s"$name say hello")
  }

  override def hurt(): Unit = {
    println("伤害方式1....")
  }
}



