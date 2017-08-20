package com.training.spark

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class ScalaFunctionPassingTest extends FunSuite with SharedSparkContext{
  test("Scala Function and Field Passing") {
    val fileName = "README.md"
    val outputFileLocation = makeTargetDirectory()
    val fileLocation = new File((getClass.getClassLoader.getResource(fileName).toURI)).getAbsoluteFile().toString
    val func = new ScalaFunctionPassing(sc, fileLocation, outputFileLocation, "python")
    val totalCount = func.countMatches()
    val expectedFilterTotal = 30
    val expectedTotal = 262

    val actualMatchesFieldReferenceTotal = totalCount.getMatchesFieldReference()
    val actualMatchesFunctionReferenceTotal = totalCount.getMatchesFunctionReference()
    val actualMatchesNoReferenceTotal = totalCount.getMatchesNoReference()

    assert(expectedTotal == actualMatchesFieldReferenceTotal)
    assert(expectedFilterTotal == actualMatchesFunctionReferenceTotal)
    assert(expectedTotal == actualMatchesNoReferenceTotal)
  }

  def makeTargetDirectory():String = {
    val testSubDirectory = new File("target/outputFileLocation")
    if (testSubDirectory.exists()) testSubDirectory.delete()

    testSubDirectory.mkdirs()

    testSubDirectory.getAbsolutePath
  }

}
