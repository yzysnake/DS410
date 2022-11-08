case class Neumaier(sum: Double, c: Double)

import scala.math.abs

object HW {

   def q1_countsorted(x: Int, y: Int, z:Int) : Int = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      if (x < y) {
         if (y < z){
            if (x < z)
               {3}
         else {2}
         }
         else {1}
      }
      else {0}
   }

   def q2_interpolation(name: String, age: Int) : String = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      if (age >= 21) {"hello, " + name.toLowerCase} 
         else {"hodwy, " + name.toLowerCase}
   }

   def q3_polynomial(arr: Seq[Double]) : Double = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      arr.foldLeft((0,0.0)){(a,x) => (a._1 + 1, a._2 + x * (a._1 + 1))}._2
   }

   def q4_application(x: Int, y: Int, z: Int)(f: (Int, Int) => Int) = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      f(f(x,y),z)
   }

   // create the rest of the functions yourself
   // in order for the code to compile, you need to (at the very least) create
   // blank versions of the remaining functions and have them return a value of 
   // the expected type, like the blank functions above.
   // remember, to compile, you don't specify any file names, you just use sbt compile
   def q5_stringy(start: Int, n: Int) : Vector[String] = {
      Vector.tabulate(n){x => (x + start).toString}
   }

   def q6_modab(a: Int, b: Int, c:Vector[Int]) : Vector[Int] = {
      c.filter(x => x >= a).filter(x => x%b != 0)
   }

   
   def q7_count(arr:Vector[Int])(f: Int => Boolean) : Int = {
      val current = arr.head
      val next = arr.tail

      if (next.isEmpty) {
         if (f(current)){1}
         else{0}
      }
      else{
         if (f(current)) {1 + q7_count(next)(f)}
         else{q7_count(next)(f)}
      }
   }

   @annotation.tailrec
   def q8_count_tail(arr:Vector[Int], counter:Int = 0)(f: Int => Boolean) : Int = {
      
      val current = arr.head
      val next = arr.tail

      if (next.isEmpty) {
         if (f(current)){counter + 1}
         else{counter}
      }
      else{
         if (f(current)) {q8_count_tail(next,counter + 1)(f)}
         else{q8_count_tail(next,counter)(f)}
      }
   }

   def q9_neumaier(arr: Seq[Double]): Double = {
      val temp = arr.foldLeft(Neumaier(0.0, 0.0)){(s, x) => {
         val t = s.sum + x
         if (abs(s.sum) >= abs(x)) {
          Neumaier(t, (s.sum - t) + x)
         } else {
          Neumaier(t, (x - t) + s.sum)
         }
      }}
      temp.sum + temp.c
  }

}