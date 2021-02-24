package com.gin

import scala.collection.immutable

object Lv02_If_Where {


  //自己特征：class  object
  //流控
  def main(args: Array[String]): Unit = {

    //if
    var a = 0
    if (a <= 0) {
      println(s"$a<0")
    } else {
      println(s"$a>=0")
    }
    println("-----------------------------")

    //while
    var aa = 0
    while (aa < 5) {
      println(aa)
      aa = aa + 1
    }

    println("-----------------------------")

    //scala 只支持集合迭代for循环
    //定义集合, 从1开始,步长为2,到10结束
    //类似shell:
    // for i in `seq 10`; do echo $i; done
    val seq: Range.Inclusive = 1 to(10, 2)
    println(seq)

    // to 和 until区别, until不取最后一位: 10
    val seqs = 1 until 10
    println(seqs)

    //循环逻辑，业务逻辑
    //for中if判断(循环逻辑)
    for (i <- seqs if (i % 2 == 0)) {
      //打印偶数(业务逻辑)
      println(i)
    }

    println("-----------------------------")

    //scala实现1
    /*for (i <- 1 to 9) {
      for (j <- 1 to 9) {
        if (j <= i) print(s"$i * $j = ${i * j}\t")
        if (j == i) println()
      }
    }*/

    //scala实现2
    var num = 0
    //一个for中, 两个序列迭代(双重循环)
    //优势在于通过守卫条件判断,减少了无用循环
    for (i <- 1 to 9; j <- 1 to 9 if (j <= i)) {
      num += 1
      //输出乘法计算过程
      if (j <= i) print(s"$i * $j = ${i * j}\t")
      //两者相同则换行
      if (j == i) println()
    }
    println(s"迭代次数: $num")

    println("-----------------------------")

    val seqss: immutable.IndexedSeq[Int] = for (i <- 1 to 10) yield {
      var x = 1
      i + x
    }
    println(seqss)

    println("-----------------------------")
    for (i <- seqss) {
      print(s"$i ")
    }
    println()
    println("-----------------------------")

  }

}
