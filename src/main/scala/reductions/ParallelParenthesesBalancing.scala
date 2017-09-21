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
    def balanceCount(chars: Array[Char], cnt: Int): Int = {
      if (cnt == -1) -1
      else if (chars.isEmpty) cnt
      else if (chars.head == '(') balanceCount(chars.tail, cnt + 1)
      else if (chars.head == ')') balanceCount(chars.tail, cnt - 1)
      else balanceCount(chars.tail, cnt)
    }

    balanceCount(chars, 0) == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, unmatchedOpenPar: Int, unmatchedClosePar: Int): (Int, Int) = {
      if (idx < until) {
        chars(idx) match {
          case '(' => traverse(idx + 1, until, unmatchedOpenPar + 1, unmatchedClosePar)
          case ')' =>
            if (unmatchedOpenPar > 0) traverse(idx + 1, until, unmatchedOpenPar - 1, unmatchedClosePar)
            else traverse(idx + 1, until, unmatchedOpenPar, unmatchedClosePar + 1)
          case _ => traverse(idx + 1, until, unmatchedOpenPar, unmatchedClosePar)
        }
      } else (unmatchedOpenPar, unmatchedClosePar)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      val size = until - from
      if (size > threshold) {
        val halfSize = size / 2
        val ((a1, a2), (b1, b2)) = parallel(reduce(from, from + halfSize), reduce(from + halfSize, until))
        // ))(( (0,2) (2,0) => (2,2) These cannot be reduced
        if (a1 > b2) { // In this case all b2 unmatched can be matched with a1 but
          (a1 - b2 + b1 , a2) // a2 cannot be matched with b1
        } else { // In this case all a1 unmatched (if any) can be matched with b2 but
          (b1, b2 - a1 + a2) // b1 cannot be matched with a2
        }
      }
      else {
        traverse(from, until, 0, 0)
      }
    }
    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
