package com.gin

/**
  * object 约等于 static 单例对象
  * static
  * scala的编译器很人性化: 节省代码量
  * object class 修饰同名类会形成伴生类, 私有化资源也可共享
  *
  */
object Lv01_Object_Class {

  //指定类型
  //private val myClass: Lv01_Class = new Lv01_Class(11)
  //不指定类型
  //private val myClass = new Lv01_Class("女")
  private val myClass = new Lv01_Object_Class(18)

  //  var/val
  //  var:变量
  //  val:常量 取代了final
  //val name = "Lv01_Object:李四"
  private val name = "Lv01_Object:李四"
  var age = 0

  //类里，裸露的代码是默认构造中的(有默认构造)
  //会被封装到默认构造方法里
  println("Lv01_Object....up")

  def main(args: Array[String]): Unit = {
    println("hello from Lv01_Object")
    myClass.printMsg()
  }

  //会被封装到默认构造方法里
  println("Lv01_Object....down")

}

/**
  * 注意, object 和 class 关键字修饰同一个文件名时,
  * 那么会生成伴生类, 实际会编译在同一个文件下, 同时可以共享变量
  *
  * 类里，裸露的代码是默认构造中的(有默认构造)
  * 类名构造器中的参数就是类的成员属性，且默认是val类型，且默认是private
  *
  * @param sex 默认构造参数
  */

//只有在类名构造其中的参数可以设置成var，其他方法函数中的参数都是val类型的，且不允许设置成var类型
//class Lv01_Class(var sex: String) {
class Lv01_Object_Class(sex: String) {

  var name = "Lv01_Class:张三"

  //自定义构造
  def this(age: Int) {
    //类名上没有定义常量属性时, 可以调用 this() 默认构造
    //this()
    //必须调用默认构造, 这里的默认构造需要设置 sex 常量
    this("女")
    this.age = age
  }

  var age: Int = 8

  //字符串前添加 s 可以通过 $ 符号进行取值
  println(s"Lv01_Class....up$age....")

  def printMsg(): Unit = {
    println(s"sex: ${this.name}")
    println(s"sex: $sex")
    //可以获取伴生类的属性值,即便该值被 private 修饰
    println(s"sex: ${Lv01_Object_Class.name}")
  }

  println(s"Lv01_Class....up${age + 4}")
}






