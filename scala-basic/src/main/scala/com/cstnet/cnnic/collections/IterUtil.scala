package com.cstnet.cnnic.collections

/**
 * Created by Tao Bian on 2016/5/18.
 * all collections are inherited from trait Iterable
 */

trait BasicSort {
  def sortList(lst: List[Int])

  def sortArray(arr: Array[Int])
}

class BubbleSort extends BasicSort {
  override def sortList(lst: List[Int]): Unit = {

  }

  override def sortArray(arr: Array[Int]): Unit = {
    for (i <- 0 to arr.length - 2; j <- 0 to arr.length - 2 - i) {
      if (arr(j) > arr(j + 1)) {
        val temp = arr(j)
        arr(j) = arr(j + 1)
        arr(j + 1) = temp
      }
    }
  }

  override def toString = s"BubbleSort()"
}

class QuickSort extends BasicSort {
  override def sortList(lst: List[Int]): Unit = {

  }

  override def sortArray(arr: Array[Int]): Unit = {
    realSort(arr, 0, arr.length - 1)
  }

  private def realSort(arr: Array[Int], start: Int, end: Int): Unit = {
    var l = start
    var h = end
    var povit = arr(l)

    while (l < h) {
      while (l < h && arr(h) >= povit) h -= 1
      if (l < h) {
        val temp = arr(h)
        arr(h) = arr(l)
        arr(l) = temp
      }

      while (l < h && arr(l) <= povit) l += 1
      if (l < h) {
        val temp = arr(l)
        arr(l) = arr(h)
        arr(h) = temp
      }
    }

    if (l > start)  realSort(arr, start, l - 1)
    if (h < end)  realSort(arr, l+1, end)
  }

  override def toString = s"QuickSort()"
}

object IterUtil {

  private def TestBubble: Unit = {
    val bubble: BasicSort = new BubbleSort
    println(bubble.toString)

    var arr = Array(2, 5, 7, 5, 3, 2)
    bubble.sortArray(arr)
  }

  private def TestQuick: Unit = {
    val quick: BasicSort = new QuickSort
    println(quick.toString)

    var arr = Array(2, 5, 7, 5, 3, 2)
    quick.sortArray(arr)
    arr.foreach(a => print(a + " "))
  }

  def main(args: Array[String]) {
//    TestBubble
    TestQuick
  }

}

