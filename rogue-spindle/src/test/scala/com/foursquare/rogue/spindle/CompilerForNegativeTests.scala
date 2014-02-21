package com.foursquare.rogue.spindle

import java.io.{PrintWriter, Writer}
import org.specs2.matcher.JUnitMustMatchers
import scala.tools.nsc.{Settings, interpreter => IR}

class CompilerForNegativeTests(seedStatements: List[String]) extends JUnitMustMatchers {

  class NullWriter extends Writer {
    override def close() = ()
    override def flush() = ()
    override def write(arr: Array[Char], x: Int, y: Int): Unit = ()
  }

  private val settings = new Settings
  settings.usejavacp.value = true
  settings.deprecation.value = true // enable detailed deprecation warnings
  settings.unchecked.value = true // enable detailed unchecked warnings

  // This is deprecated in 2.9.x, but we need to use it for compatibility with 2.8.x
  val stringWriter = new java.io.StringWriter()
  private val interpreter =
    new IR.IMain(
      settings,
      /**
       * It's a good idea to comment out this second parameter when adding or modifying
       * tests that shouldn't compile, to make sure that the tests don't compile for the
       * right reason.
       **/
      new PrintWriter(stringWriter))

  seedStatements.foreach(interpreter.interpret _)

  def typeCheck(code: String): Option[String] = {
    stringWriter.getBuffer.delete(0, stringWriter.getBuffer.length)
    val thunked = "() => { %s }".format(code)
    interpreter.interpret(thunked) match {
      case IR.Results.Success => None
      case IR.Results.Error => Some(stringWriter.toString)
      case IR.Results.Incomplete => throw new Exception("Incomplete code snippet")
    }
  }

  def check(code: String, expectedErrorREOpt: Option[String] = Some("")): Unit = {
    (expectedErrorREOpt, typeCheck(code)) aka "'%s' compiles or fails with the right message!".format(code) must beLike {
      case (Some(expectedErrorRE), Some(actualError)) => expectedErrorRE.r.findFirstIn(actualError.replaceAll("\n", "")).isDefined must beTrue
      case (None, None) => true must beTrue
    }
  }
}
