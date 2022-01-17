package com.isgneuro.zeppelinotl

import com.isgneuro.otp.connector.utils.Core.ConnectionInfo
import com.isgneuro.otp.connector.utils.Datetime.{ convertTimeToMillis, getCurrentTime }
import com.isgneuro.otp.connector.{ Connector, Dataset }
import org.apache.zeppelin.interpreter.Interpreter.FormType
import org.apache.zeppelin.interpreter.InterpreterResult.{ Code, Type }
import org.apache.zeppelin.interpreter.{ Interpreter, InterpreterContext, InterpreterResult }
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

import java.util.Properties
import scala.util.{ Failure, Success, Try }

class OTLInterpreter(properties: Properties) extends Interpreter(properties) {
  val host: Option[String] = Option(properties.getProperty("OTP.rest.host"))
  val port: Option[String] = Option(properties.getProperty("OTP.rest.port"))
  val username: Option[String] = Option(properties.getProperty("OTP.rest.auth.username"))
  val password: Option[String] = Option(properties.getProperty("OTP.rest.auth.password"))
  val datasetHost: Option[String] = Option(properties.getProperty("OTP.rest.cache.host"))
  val datasetPort: String = Option(properties.getProperty("OTP.rest.cache.port")).getOrElse("80")
  val timeout: Int = Try(properties.getProperty("OTP.query.timeout").toInt).getOrElse(300)
  val ttl: Int = Try(properties.getProperty("OTP.query.ttl").toInt).getOrElse(60)
  val convertTsToMillis: Boolean = Try(properties.getProperty("OTP.query.convertTime").toBoolean).getOrElse(true)
  val maxResultRows: Int = Try(properties.getProperty("OTP.query.maxResultRows").toInt).getOrElse(Int.MaxValue)

  val connectionInfoOption: Option[ConnectionInfo] = if (port.isDefined && host.isDefined)
    Some(ConnectionInfo(host.get, port.get, ssl = false)) else None
  val sid: Int = getCurrentTime

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def interpret(s: String, interpreterContext: InterpreterContext): InterpreterResult = {

    connectionInfoOption match {
      case Some(connectionInfo: ConnectionInfo) =>
        val query = Try {
          Query(s)
            .setTokens(interpreterContext.getResourcePool)
            .getCacheId
            .convertTimeRange
        }

        val dataset = Try {
          if (username.isDefined && password.isDefined) {
            val conn = Connector(connectionInfo, username.get, password.get)
            if (query.isSuccess) {
              val tws = query.get.earliest
              val twf = query.get.latest
              conn.jobs.create(query.get.query, ttl, tws, twf, sid, timeout).getDataset(datasetHost.getOrElse(connectionInfo.host), datasetPort)
            } else throw new Exception(
              s"""Error while converting query (setting tokens from resource pool,
                |converting time range, parsing zput command) : ${
                query match { case Failure(ex) => ex.getMessage }
              }""".stripMargin)
          } else throw new Exception(
            {
              if (username.isEmpty) "Please specify OTP.rest.auth.username in interpreter properties"
              else "Please specify OTP.rest.auth.password in interpreter properties"
            })
        }

        val result: Result = dataset match {
          case Success(ds) if ds.jsonLines.exists(_.nonEmpty) =>
            val nonEmptyDS: String = parseEvents(ds.jsonLines.take(maxResultRows), ds.schema)
            val ir = new InterpreterResult(Code.SUCCESS, Type.TABLE, nonEmptyDS)
            Result(ir, Some(ds))
          case Success(emptyDS) =>
            val ir = new InterpreterResult(Code.SUCCESS, Type.TEXT, "Resulting dataset is empty")
            Result(ir, Some(emptyDS))
          case Failure(ex) => Result(new InterpreterResult(Code.ERROR, Type.TEXT, ex.getMessage), None)
        }

        if (query.isSuccess)
          query.get.cacheId.foreach(cid =>
            result.dataset.map(ds =>
              interpreterContext.getResourcePool.put(cid, "[" + ds.jsonLines.mkString(", ") + "]")))

        result.interpreterResult

      case None => new InterpreterResult(
        Code.ERROR,
        Type.TEXT,
        {
          if (host.isEmpty) "Please specify OTP.rest.host in interpreter properties"
          else "Please specify OTP.rest.port in interpreter properties"
        })
    }

  }

  override def cancel(interpreterContext: InterpreterContext): Unit = {}

  override def getFormType: Interpreter.FormType = FormType.SIMPLE

  override def getProgress(interpreterContext: InterpreterContext): Int = 0

  /**
   * Convert resulting dataset from JSON lines to String in special format for InterpreterResult
   *
   * @param dataset - initial dataset as sequence of JSON Lines
   * @param schema  - schema of dataset
   * @return - dataframe as string; rows are separated with \n, columns - with \t
   */
  def parseEvents(dataset: Seq[String], schema: String): String = {
    val fieldPattern = "`(.+?)`".r
    implicit val formats: DefaultFormats.type = DefaultFormats
    val eventsArrayMap = dataset
      .map(
        x => parse(x)
          .extract[Map[String, Any]]
          .mapValues {
            case vv: List[Any] => vv.toString.replaceAll("List", "")
            case other => other.toString
          }
          .map { case (k, v) => k -> (if ((k == "_time") && convertTsToMillis) convertTimeToMillis(v) else v) })
    val fields = fieldPattern.findAllMatchIn(schema).map(f => f.group(1)).toList
    val headers = fields.mkString("\t")
    val values = eventsArrayMap.map(line => {
      val orderedLine = fields.foldLeft("") {
        (ol, f) =>
          ol + "\t" + line.getOrElse(f, "")
      }
      orderedLine.tail
    }).mkString("\n")
    headers + "\n" + values
  }
}
