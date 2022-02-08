package com.isgneuro.zeppelinotl

import com.isgneuro.otp.connector.Core.Dataset
import com.isgneuro.otp.connector.Core
import com.isgneuro.otp.connector.utils.Exceptions.QueryExecutionException
import com.isgneuro.zeppelinotl.OTLInterpreterMock.DatasetMock

import java.util.Properties
import scala.util.{ Failure, Success, Try }

class OTLInterpreterMock(properties: Properties) extends OTLInterpreter(properties) {

  override def createJobAndReturnDataset(query: String, connectionInfo: Core.ConnectionInfo, username: String, password: String): Try[Dataset] = {
    query match {
      case str if str.equals("| makeresults ") =>
        val datasetJsonLines = Seq("""{"_time":1643889830}""")
        val datasetSchema = """`_time` BIGINT"""
        Success(DatasetMock(datasetSchema, datasetJsonLines))

      case str if str.equals("makeresults count=2 | eval x=2 | eval y=x*x | fields x, y") =>
        val datasetJsonLines = Seq("""{"x":2,"y":4}""", """{"x":2,"y":4}""")
        val datasetSchema = """`x` INT,`y` INT"""
        Success(DatasetMock(datasetSchema, datasetJsonLines))

      case str if str.equals("makeresults count=2 | streamstats count as x | stats list(x) as xs") =>
        val datasetJsonLines = Seq("""{"xs":[1,2]}""")
        val datasetSchema = """`xs` ARRAY<BIGINT>"""
        Success(DatasetMock(datasetSchema, datasetJsonLines))

      case str if str.equals("| makeresults count=3 | streamstats count as x | eval y=x*x | fields x, y") =>
        val datasetJsonLines = Seq("""{"x":1,"y":1}""", """{"x":2,"y":4}""", """{"x":3,"y":9}""")
        val datasetSchema = """`x` BIGINT,`y` BIGINT"""
        Success(DatasetMock(datasetSchema, datasetJsonLines))

      case str if str.equals("""
                               | makeresults count=1000000
                               | streamstats count as x
                               | eval y = x * 20
                               | eventstats min(y) as miny
                               | stats min(miny) as minminy""".replace("\n", "")) =>
        val datasetJsonLines = Seq("""{"minminy":20}""")
        val datasetSchema = """`minminy` BIGINT"""
        Success(DatasetMock(datasetSchema, datasetJsonLines))

      case str if str.equals("| makeresults count=3 | where fields + 0") =>
        Failure(new QueryExecutionException("""[SearchId:228488] Error in  'where' command"""))

      case str if str.equals("| makeresults  count =  2 | eval x=1 | fields x") =>
        val datasetJsonLines = Seq("""{"x":1}""", """{"x":1}""")
        val datasetSchema = """`x` INT"""
        Success(DatasetMock(datasetSchema, datasetJsonLines))

      case str if str.equals("| makeresults count=10 | streamstats count as x | eval y=1 | fields x, y") =>
        val datasetJsonLines = Seq("""{"x":1,"y":1}""", """{"x":2,"y":1}""", """{"x":3,"y":1}""", """{"x":4,"y":1}""",
          """{"x":5,"y":1}""", """{"x":6,"y":1}""", """{"x":7,"y":1}""", """{"x":8,"y":1}""", """{"x":9,"y":1}""",
          """{"x":10,"y":1}""")
        val datasetSchema = """`x` BIGINT,`y` INT"""
        Success(DatasetMock(datasetSchema, datasetJsonLines))

      case str if str.equals("| makeresults count =3 | eval a = 1 | search a<0 ") =>
        val datasetJsonLines = Seq.empty[String]
        val datasetSchema = """`_time` BIGINT,`a` INT"""
        Success(DatasetMock(datasetSchema, datasetJsonLines))

    }
  }

}

object OTLInterpreterMock {

  case class DatasetMock(datasetSchema: String, datasetLines: Seq[String]) extends Dataset {
    override val schema: String = datasetSchema
    override val jsonLines: Seq[String] = datasetLines
  }

}