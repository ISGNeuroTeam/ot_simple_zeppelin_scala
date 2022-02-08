package com.isgneuro.zeppelinotl

import java.util.Properties
import org.apache.zeppelin.interpreter.InterpreterContext
import org.scalatest.{ FlatSpec, Matchers }

class OTLInterpreterTest extends FlatSpec with Matchers {

  val propertiesMap: Map[String, String] = Map(
    "OTP.rest.host" -> "192.168.4.65",
    "OTP.rest.port" -> "80",
    "OTP.rest.auth.username" -> "admin",
    "OTP.rest.auth.password" -> "12345678",
    "OTP.rest.cache.host" -> "192.168.4.65",
    "OTP.rest.cache.port" -> "80",
    "OTP.query.timeout" -> "60",
    "OTP.query.ttl" -> "60",
    "OTP.query.convertTime" -> "false")

  val properties: Properties = {
    val properties = new Properties()
    propertiesMap.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  val interpreterContext: InterpreterContext = new InterpreterContext.Builder().build

  "Mock" should "work fine" in {
    val mockInterpreter = new OTLInterpreterMock(properties)

    val result = mockInterpreter.interpret("| makeresults ", interpreterContext)

    val expected = "%table _time\n1643889830"

    result.toString should be(expected)

  }

  it should "parse json lines from dataset and convert them to appropriate format" in {
    val mockInterpreter = new OTLInterpreterMock(properties)

    val result = mockInterpreter.interpret("makeresults count=2 | eval x=2 | eval y=x*x | fields x, y", interpreterContext)

    val expected = "%table x\ty\n2\t4\n2\t4"

    result.toString should be(expected)
  }

  it should "convert List to correct string without 'List' word in resulting dataset" in {
    val mockInterpreter = new OTLInterpreterMock(properties)

    val result = mockInterpreter.interpret("makeresults count=2 | streamstats count as x | stats list(x) as xs", interpreterContext)

    val expected = "%table xs\n(1, 2)"

    result.toString should be(expected)
  }

  it should "get query as string, run Connector, get and parse resulting dataset" in {
    val mockInterpreter = new OTLInterpreterMock(properties)

    val result = mockInterpreter.interpret("| makeresults count=3 | streamstats count as x | eval y=x*x | fields x, y", interpreterContext)

    val expected = "%table x\ty\n1\t1\n2\t4\n3\t9"

    result.toString should be(expected)
  }

  it should "return an error if query fails due to timeout" in {

    properties.setProperty("OTP.query.timeout", "-1")

    val mockInterpreter = new OTLInterpreterMock(properties)

    val result = mockInterpreter.interpret("""
                                             | makeresults count=1000000
                                             | streamstats count as x
                                             | eval y = x * 20
                                             | eventstats min(y) as miny
                                             | stats min(miny) as minminy
      """.replace("\n", ""), interpreterContext)
    properties.setProperty("OTP.query.timeout", "60")

    result.code().toString should be("ERROR")
  }

  it should "return an error if query fails due to execution error" in {

    val mockInterpreter = new OTLInterpreterMock(properties)

    val result = mockInterpreter.interpret("| makeresults count=3 | where fields + 0", interpreterContext)

    result.code().toString should be("ERROR")

  }

  it should "convert _time column to millisecs in resulting DF according to OTP.query.convertTime property" in {

    properties.setProperty("OTP.query.convertTime", "true")

    val result1 = new OTLInterpreterMock(properties).interpret("| makeresults ", interpreterContext)
    val expected1 = "%table _time\n1643889830000"

    properties.setProperty("OTP.query.convertTime", "false")

    val result2 = new OTLInterpreterMock(properties).interpret("| makeresults ", interpreterContext)
    val expected2 = "%table _time\n1643889830"

    result1.toString should be(expected1)
    result2.toString should be(expected2)
  }

  it should "get tokens' values from resource pool and put them into query" in {
    val query = "| makeresults  count = $count$ | eval x=1 | fields x"

    val ctxRP = InterpreterContextHelper.setResourcePool(interpreterContext)

    ctxRP.getResourcePool.put("count", "2")

    val expected = "| makeresults  count = 2 | eval x=1 | fields x"

    Query(query).setTokens(ctxRP.getResourcePool).query should be(expected)
  }

  it should "execute corrected query with tokens' values from resource pool" in {

    val ctxRP = InterpreterContextHelper.setResourcePool(interpreterContext)
    ctxRP.getResourcePool.put("count", "2")

    val mockInterpreter = new OTLInterpreterMock(properties)

    val result = mockInterpreter.interpret("| makeresults  count =  $count$ | eval x=1 | fields x", ctxRP)

    val expected = "%table x\n1\n1"

    result.toString should be(expected)

  }

  it should "limit resulting dataset to value of maxResultRows" in {

    properties.setProperty("OTP.query.maxResultRows", "3")

    val mockInterpreter = new OTLInterpreterMock(properties)

    val result = mockInterpreter.interpret("| makeresults count=10 | streamstats count as x | eval y=1 | fields x, y", interpreterContext)

    val expected = "%table x\ty\n1\t1\n2\t1\n3\t1"

    result.toString should be(expected)

    properties.remove("OTP.query.maxResultRows")

  }

  it should "return a message if resulting dataset is empty" in {
    val mockInterpreter = new OTLInterpreterMock(properties)

    val result = mockInterpreter.interpret("| makeresults count =3 | eval a = 1 | search a<0 ", interpreterContext)
    val expected = "%text Resulting dataset is empty"

    result.toString should be(expected)
  }
}
