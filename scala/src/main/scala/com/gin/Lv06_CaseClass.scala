package com.gin


object Lv06_CaseClass {

  def main(args: Array[String]): Unit = {
    val dog1 = Dog("hsq", 18)
    val dog2 = Dog("hsq", 18)
    //在 case class 中, 可以使同数据对象判断为相等
    println(dog1.equals(dog2))
    println(dog1 == dog2)
  }

}

/**
  * case 便于判断对象属性是否相同
  * 适用场景: 分布式环境下的数据传递, 判断数据一致性
  */
case class Dog(name: String, age: Int) {

}

