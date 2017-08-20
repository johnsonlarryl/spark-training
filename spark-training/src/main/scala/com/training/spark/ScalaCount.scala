package com.training.spark

/**
  * Created by lu19 on 8/20/17.
  */
class ScalaCount(val matchesFunctionReferenceCount:Long, matchesFieldReferenceCount:Long, val matchesNoReferenceCount:Long) {
  def getMatchesFunctionReference():Long = {
    matchesFunctionReferenceCount
  }

  def getMatchesFieldReference(): Long = {
    matchesFieldReferenceCount
  }

  def getMatchesNoReference():Long = {
    matchesNoReferenceCount
  }
}
