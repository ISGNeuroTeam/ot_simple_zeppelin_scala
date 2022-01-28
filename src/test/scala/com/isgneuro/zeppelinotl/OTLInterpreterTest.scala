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

  "OTLInterpreter" should "fail if incorrect credentials specified in propeties" in pendingUntilFixed {
    properties.setProperty("OTP.rest.auth.password", "wrongPass")
    val query = "| makeresults"
    val interp = new OTLInterpreter(properties)
    val res = interp.interpret(query, ctx)
    res.code().toString should be("ERROR")
    res.toString should include("Fail to authenticate")
    properties.setProperty("OTP.rest.auth.password", "12345678")
  }

  it should "convert 'earliest' and 'latest' to unix time" in {
    val query = "| getdata object=devices_h  earliest =  2019/01/01:0:0:0 latest= 2020/01/01:0:0:0 | stats min(_time) as mint"
    val expected = "| getdata object=devices_h  earliest =  1546290000 latest= 1577826000 | stats min(_time) as mint"
    Query(query).convertTimeRange.query should be(expected)
  }

  it should "not convert 'earliest' and 'latest' to unix time if they are not in the first command" in {
    val query = """makeresults | eval f1 = "earliest = 2019/01/01:0:0:0" | eval f2 = "latest=2020/01/01:0:0:0" """
    val expected = query
    Query(query).convertTimeRange.query should be(expected)
  }

  it should "extract time range from query if specified" in {
    val query = "| getdata object=devices_h  earliest = 2019/01/01:0:0:0  latest  =2020/01/01:0:0:0 | stats min(_time) as mint"
    Query(query).earliest should be(1546290000)
    Query(query).latest should be(1577826000)
  }

  it should "parse json lines from dataset and convert them to appropriate format" in pendingUntilFixed {
    val query = "makeresults count=2 | eval x=2 | eval y=x*x | fields x, y"
    val connInfo = ConnectionInfo(properties.getProperty("OTP.rest.host"), properties.getProperty("OTP.rest.port"), ssl = false)
    val conn = Connector(connInfo, properties.getProperty("OTP.rest.auth.username"), properties.getProperty("OTP.rest.auth.password"))
    val dataset = conn.jobs.create(query).getDataset
    val expected = "x\ty\n2\t4\n2\t4"
    val interp = new OTLInterpreter(properties)
    interp.parseEvents(dataset.jsonLines, dataset.schema) should be(expected)
  }

  it should "convert List to correct string without 'List' word in resulting dataset" in pendingUntilFixed {
    val query = "makeresults count=2 | streamstats count as x | stats list(x) as xs"
    val interp = new OTLInterpreter(properties)
    val res = interp.interpret(query, ctx)
    res.toJson should include(""""data":"xs\n(1, 2)"""")
  }

  it should "get query as string, run Connector, get and parse resulting dataset" in pendingUntilFixed {
    val query = "| makeresults count=3 | streamstats count as x | eval y=x*x | fields x, y"
    val interp = new OTLInterpreter(properties)
    val res = interp.interpret(query, ctx)
    res.code().toString should be("SUCCESS")
    res.toString should include("table")
    res.toJson should include(""""data":"x\ty\n1\t1\n2\t4\n3\t9"""")
  }

  it should "return an error if query fails due to timeout" in pendingUntilFixed {
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

  it should "return an error if query fails due to execution error" in pendingUntilFixed {
    val query = "| makeresults count=3 | where fields + 0"
    val interp = new OTLInterpreter(properties)
    val res = interp.interpret(query, ctx)
    res.code().toString should be("ERROR")
    res.toString should include("'where' command")
  }

  it should "convert _time column to millisecs in resulting DF according to OTP.query.convertTime property" in pendingUntilFixed {
    val query = "| makeresults "
    properties.setProperty("OTP.query.convertTime", "true")
    val interpTrue = new OTLInterpreter(properties)
    val dsTrue = interpTrue.interpret(query, ctx)
    val resultValueTrue = dsTrue.toString.split("\n").last
    resultValueTrue.toLong % 1000 should be(0)
    resultValueTrue should have length 13

    properties.setProperty("OTP.query.convertTime", "false")
    val interpFalse = new OTLInterpreter(properties)
    val dsFalse = interpFalse.interpret(query, ctx)
    val resultValueFalse = dsFalse.toString.split("\n").last
    resultValueFalse should have length 10
  }

  it should "extract cache ID from query" in {
    val query = "| makeresults | zput  id  =  test123  | eval a=1"
    val cleanedQuery = Query(query).getCacheId
    cleanedQuery.query should be("| makeresults | eval a=1")
    cleanedQuery.cacheId should be('defined)
    cleanedQuery.cacheId.get should be("test123")
  }

  it should "return the same query if no cacheId specified in the end of query" in {
    val query = "| makeresults | eval a=1"
    val cleanedQuery = Query(query).getCacheId
    cleanedQuery.query should be(query)
    cleanedQuery.cacheId should be(None)
  }

  it should "put dataset to cache if cacheId specified in query" in pendingUntilFixed {
    val query = "| makeresults count=2 | eval a=1 | fields a | zput  id =      test1"
    val ctxRP = InterpreterContextHelper.setResourcePool(ctx)
    val interp = new OTLInterpreter(properties)
    val _ = interp.interpret(query, ctxRP)
    val expected = """[{"a":1}, {"a":1}]"""
    ctxRP.getResourcePool.get("test1").get.toString should be(expected)
  }

  it should "get tokens' values from resource pool and put them into query" in {
    val query = "| makeresults  count = $count$ | eval x=1 | fields x"
    val ctxRP = InterpreterContextHelper.setResourcePool(ctx)
    ctxRP.getResourcePool.put("count", "2")
    val expected = "| makeresults  count = 2 | eval x=1 | fields x"
    Query(query).setTokens(ctxRP.getResourcePool).query should be(expected)
  }

  it should "execute corrected query with tokens' values from resource pool" in pendingUntilFixed {
    val query = "| makeresults  count =  $count$ | eval x=1 | fields x"
    val ctxRP = InterpreterContextHelper.setResourcePool(ctx)
    ctxRP.getResourcePool.put("count", "2")
    val interp = new OTLInterpreter(properties)
    interp.interpret(query, ctxRP).toJson should include(""""data":"x\n1\n1"}""")
  }

  it should "limit resulting dataset to value of maxResultRows" in pendingUntilFixed {
    properties.setProperty("OTP.query.maxResultRows", "3")

    val query = "| makeresults count=10 | streamstats count as x | eval y=1 | fields x, y"
    val interp = new OTLInterpreter(properties)
    val res = interp.interpret(query, ctx)
    res.code().toString should be("SUCCESS")
    res.toString should include("table")
    res.toJson should include(""""data":"x\ty\n1\t1\n2\t1\n3\t1"""")

    properties.remove("OTP.query.maxResultRows")
  }

  it should "return a message if resulting dataset is empty" in pendingUntilFixed {
    val query = "| makeresults count =3 | eval a = 1 | search a<0 "
    val interp = new OTLInterpreter(properties)
    val res = interp.interpret(query, ctx)
    res.code().toString should be("SUCCESS")
    res.toString should include("Resulting dataset is empty")
  }
}
