package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    var i = 0
    var count = 0
    val max = chars.length
    while (i < max) {
      val c = chars(i)
      if (c == '(')
        count = count + 1
      else if (c == ')') {
        count = count - 1
        if (count < 0)
          i = max
      }
      i = i + 1
    }
    count == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, count: Int, min: Int) : (Int, Int) = {
      // TODO Why are count and min pars needed?
      var i = idx
      var newCount = count
      var newMin = min
      while (i < until) {
        val c = chars(i)

        if (c == '(')
          newCount += 1
        else if (c == ')')
          newCount -= 1

        if (newCount < newMin)
          newMin = newCount

        i += 1
      }
      (newCount, newMin)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold)
        traverse(from, until, 0, 0)
      else {
        val mid = (until + from) / 2
        val ((countL, minL), (countR, minR)) = parallel(reduce(from, mid), reduce(mid, until))
        val temp = minR + countL
        (countL + countR, if (minL < temp) minL else temp)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
