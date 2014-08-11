/*
 * Copyright (c) 2014 Tushar R. Gosavi. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tugo.datatorrent.demo.pi

import com.datatorrent.api.annotation.ApplicationAnnotation
import com.datatorrent.api.Context.OperatorContext
import com.datatorrent.api._
import java.util.Random
import org.apache.hadoop.conf.Configuration
;

/**
 * Class Point represent a point in 2D space.
 *
 */
class Point(val x : Int, val y: Int) {
  // Needed for kryo serialization.
  def this() = this(0,0)

  /**
   * Calculate distance from origin (0,0)
   * @return distance from origin (double)
   */
  def dist() = x * x + y * y;

  override
  def toString = "[ " + x + ", " + y + " ]"
}

/**
 * Generate random points in 100x100 grid and sends them on
 * output port.
 *
 * you can control the speed of generation with tuppleBlast and
 * sleepTime parameters.
 */
class RandomIntGenerator extends InputOperator {

  override def teardown(): Unit = {}

  override def setup(p1: OperatorContext): Unit = {}

  override def endWindow(): Unit = {}

  override def beginWindow(p1: Long): Unit = {}

  @transient
  val out : DefaultOutputPort[Point] = new DefaultOutputPort[Point]();

  var tupleBlast = 100
  var sleepTime = 10
  val r = new Random()

  override def emitTuples(): Unit = {
    for (i <- 1 to tupleBlast) {
      val x = r.nextInt(100)
      val y = r.nextInt(100)

      out.emit(new Point(x, y))
    }
    if (sleepTime != 0)
      Thread.sleep(sleepTime)
  }
}

/**
 * Calculate value of over life time of the application,
 * It emits new value of PI at end window.
 */
class PiCalculator extends BaseOperator {
  val base = 100 * 100
  var inArea = 0
  var totalArea = 0

  @transient
  val in : DefaultInputPort[Point] = new DefaultInputPort[Point] {
    override def process(p: Point): Unit = {
      if (p.dist() < base)
        inArea = inArea + 1
      totalArea = totalArea + 1
    }
  }

  @transient
  val out : DefaultOutputPort[Double] = new DefaultOutputPort[Double]

  override def endWindow() {
    val result = (inArea.toDouble / totalArea.toDouble) * 4.0
    out.emit(result)
  }
}

/**
 * Write value on the console For some reason I am not able
 * to reuse ConsoleOutputOperator from Malhar contrib directly
 * because of type difference.
 * @tparam T
 */
class ConsoleOutOperator[T] extends BaseOperator {
  @transient
  val in : DefaultInputPort[T] = new DefaultInputPort[T] {
    override def process(obj: T): Unit = {
      println(obj)
    }
  }
}

@ApplicationAnnotation(name="PiCalculatorScala")
class PiApplication extends StreamingApplication {

  override def populateDAG(dag: DAG, conf: Configuration): Unit = {
    val gen = dag.addOperator("gen", new RandomIntGenerator)
    val cal = dag.addOperator("cal", new PiCalculator)
    val out = dag.addOperator("out", new ConsoleOutOperator[Double])

    dag.addStream[Point]("data", gen.out, cal.in);
    dag.addStream[Double]("result", cal.out, out.in)
  }
}
