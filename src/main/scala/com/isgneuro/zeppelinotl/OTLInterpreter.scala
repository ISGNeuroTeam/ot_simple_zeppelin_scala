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

  //val connectionInfoOption: Option[ConnectionInfo] = if (port.isDefined && host.isDefined) Some(ConnectionInfo(host.get, port.get, ssl = false)) else None
  val sid: Int = getCurrentTime

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def interpret(s: String, interpreterContext: InterpreterContext): InterpreterResult = {
    if (host.isDefined && port.isDefined && username.isDefined && password.isDefined) {
      val connectionInfo = ConnectionInfo(host.get, port.get, ssl = false)
      Try(Query(s).setTokens(interpreterContext.getResourcePool)) flatMap {
        query =>
          Try(Connector(connectionInfo, username.get, password.get)) flatMap {
            connector => Try(connector.jobs.create(query.query, ttl, 0, 0, sid, timeout).getDataset(datasetHost.getOrElse(connectionInfo.host), datasetPort))
          }
      } flatMap {
        dataset =>
          if (dataset.jsonLines.exists(_.nonEmpty)) Try(parseEvents(dataset.jsonLines.take(maxResultRows), dataset.schema)) else Success("")
      } match {
        case Success(interpretResult) => if (interpretResult.isEmpty) new InterpreterResult(Code.SUCCESS, Type.TEXT, "Resulting dataset is empty")
        else new InterpreterResult(Code.SUCCESS, Type.TABLE, interpretResult)
        case Failure(exception) => new InterpreterResult(Code.ERROR, Type.TEXT, s"""Caught exception '${exception.getMessage}' while interpreting""")
      }
    } else {
      new InterpreterResult(Code.ERROR, Type.TEXT,
        s"""Some interpreter parameters are not specified. Please specify 'OTP.rest.host',
           |'OTP.rest.port', 'OTP.rest.auth.username', 'OTP.rest.auth.password' in interpreter properties.
           |""".stripMargin)
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
