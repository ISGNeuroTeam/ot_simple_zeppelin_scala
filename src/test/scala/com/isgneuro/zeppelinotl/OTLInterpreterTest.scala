package com.isgneuro.zeppelinotl

import java.util.Properties
import org.apache.zeppelin.interpreter.InterpreterContext
import org.scalatest.{ FlatSpec, Matchers }
import com.isgneuro.otp.connector.Connector
import com.isgneuro.otp.connector.utils.Core.ConnectionInfo

class OTLInterpreterTest extends FlatSpec with Matchers {
  val propertiesMap: Map[String, String] = Map(
    "OTP.rest.host" -> "192.168.4.65",
    "OTP.rest.port" -> "80",
    "OTP.rest.auth.username" -> "admin",
    "OTP.rest.auth.password" -> "12345678",
    "OTP.rest.cache.host" -> "192.168.4.65",
    "OTP.rest.cache.port" -> "80",
    "OTP.query.timeout" -> "60",
    "OTP.query.ttl" -> "60")
  val properties: Properties = {
    val properties = new Properties()
    propertiesMap.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  val ctx: InterpreterContext = new InterpreterContext.Builder().build

  it should "parse json lines from dataset and convert them to appropriate format" in {

    //otlquery - "makeresults count=2 | eval x=2 | eval y=x*x | fields x, y"

    val datasetJsonLines = Seq("""{"x":2,"y":4}""", """{"x":2,"y":4}""")
    val datasetSchema = """`x` INT,`y` INT"""

    val expected = "x\ty\n2\t4\n2\t4"
    val interp = new OTLInterpreter(properties)
    interp.parseEvents(datasetJsonLines, datasetSchema) should be(expected)
  }

  it should "convert List to correct string without 'List' word in resulting dataset" in {

    //"makeresults count=2 | streamstats count as x | stats list(x) as xs"

    val datasetJsonLines = Seq("""{"xs":[1,2]}""")
    val datasetSchema = """`xs` ARRAY<BIGINT>"""

    val interpreter = new OTLInterpreter(properties)
    val result = interpreter.parseEvents(datasetJsonLines, datasetSchema)
    val expected = "xs\n(1, 2)"

    result should be(expected)
  }

  ignore should "get query as string, run Connector, get and parse resulting dataset" in {
    val query = "| makeresults count=3 | streamstats count as x | eval y=x*x | fields x, y"
    val interp = new OTLInterpreter(properties)
    val res = interp.interpret(query, ctx)
    res.code().toString should be("SUCCESS")
    res.toString should include("table")
    res.toJson should include(""""data":"x\ty\n1\t1\n2\t4\n3\t9"""")
  }

  ignore should "return an error if query fails due to timeout" in {
    properties.setProperty("OTP.query.timeout", "-1")
    val longQuery =
      """
        | makeresults count=1000000
        | streamstats count as x
        | eval y = x * 20
        | eventstats min(y) as miny
        | stats min(miny) as minminy
      """.replace("\n", "")
    val interp = new OTLInterpreter(properties)
    val res = interp.interpret(longQuery, ctx)
    properties.setProperty("OTP.query.timeout", "60") // Return timeout back for next tests
    res.code().toString should be("ERROR")
    res.toString should include("timeout")
  }

  ignore should "return an error if query fails due to execution error" in {
    val query = "| makeresults count=3 | where fields + 0"
    val interp = new OTLInterpreter(properties)
    val res = interp.interpret(query, ctx)
    res.code().toString should be("ERROR")
    res.toString should include("'where' command")
  }

  it should "convert _time column to millisecs in resulting DF according to OTP.query.convertTime property" in {

    val datasetJsonLines = Seq("""{"_time":1643889830}""")
    val datasetSchema = """`_time` BIGINT"""

    properties.setProperty("OTP.query.convertTime", "true")

    val interpretResult = new OTLInterpreter(properties).parseEvents(datasetJsonLines, datasetSchema)

    properties.setProperty("OTP.query.convertTime", "false")

    val interpretResult2 = new OTLInterpreter(properties).parseEvents(datasetJsonLines, datasetSchema)

    val expected = "_time\n1643889830000"
    val expected2 = "_time\n1643889830"

    interpretResult should be(expected)
    interpretResult2 should be(expected2)
  }

  it should "get tokens' values from resource pool and put them into query" in {
    val query = "| makeresults  count = $count$ | eval x=1 | fields x"
    val ctxRP = InterpreterContextHelper.setResourcePool(ctx)
    ctxRP.getResourcePool.put("count", "2")
    val expected = "| makeresults  count = 2 | eval x=1 | fields x"
    Query(query).setTokens(ctxRP.getResourcePool).query should be(expected)
  }

  ignore should "execute corrected query with tokens' values from resource pool" in {
    val query = "| makeresults  count =  $count$ | eval x=1 | fields x"
    val ctxRP = InterpreterContextHelper.setResourcePool(ctx)
    ctxRP.getResourcePool.put("count", "2")
    val interp = new OTLInterpreter(properties)
    interp.interpret(query, ctxRP).toJson should include(""""data":"x\n1\n1"}""")
  }

  ignore should "limit resulting dataset to value of maxResultRows" in {
    properties.setProperty("OTP.query.maxResultRows", "3")

    val query = "| makeresults count=10 | streamstats count as x | eval y=1 | fields x, y"
    val interp = new OTLInterpreter(properties)
    val res = interp.interpret(query, ctx)
    res.code().toString should be("SUCCESS")
    res.toString should include("table")
    res.toJson should include(""""data":"x\ty\n1\t1\n2\t1\n3\t1"""")

    properties.remove("OTP.query.maxResultRows")
  }

  ignore should "return a message if resulting dataset is empty" in {
    val query = "| makeresults count =3 | eval a = 1 | search a<0 "
    val interp = new OTLInterpreter(properties)
    val res = interp.interpret(query, ctx)
    res.code().toString should be("SUCCESS")
    res.toString should include("Resulting dataset is empty")
  }
}
