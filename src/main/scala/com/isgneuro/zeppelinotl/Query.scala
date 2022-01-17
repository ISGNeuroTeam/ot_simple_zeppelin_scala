package com.isgneuro.zeppelinotl

import com.isgneuro.otp.connector.utils.Datetime.getCurrentTime
import org.apache.zeppelin.resource.ResourcePool

import java.text.SimpleDateFormat
import scala.util.{ Failure, Success, Try }

case class Query(query: String, cacheId: Option[String]) {

  val timerange: Map[String, Int] = {
    val firstCommand = query.trim.stripPrefix("|").split("\\|").headOption.getOrElse("").trim
    """(earliest|latest)\s*=\s*(.*?)(\s|$|\|)""".r.findAllIn(firstCommand)
      .matchData
      .map(x => (x.group(1), x.group(2)))
      .toMap
      .mapValues(Query.convertToUnixTime)
  }
  val earliest: Int = timerange.getOrElse("earliest", 0)
  val latest: Int = timerange.getOrElse("latest", getCurrentTime)

  def convertTimeRange: Query = {
    val firstCommand = query.trim.stripPrefix("|").split("\\|").headOption.getOrElse("").trim
    val newQuery = """(earliest|latest)\s*=\s*(.*?)(\s|$|\|)""".r.findAllIn(firstCommand)
      .matchData
      .map(x => x.group(2))
      .toList
      .foldLeft(query) {
        (acc, item) => acc.replaceAllLiterally(item, Query.convertToUnixTime(item).toString)
      }
    Query(newQuery, cacheId)
  }

  /**
   * Extract cacheId, remove command "zput" from the end of query
   *
   * @return - New query with no 'zput' command and specified cacheId
   */
  def getCacheId: Query = {
    """(\|\s*zput\s+id\s*=\s*(.*?))(\||$)""".r.findAllIn(query.trim)
      .matchData
      .flatMap(x => List(x.group(0), x.group(1), x.group(2)))
      .toList match {
        case _ :: cacheCmd :: cacheId :: _ =>
          val newQuery = query.replaceAllLiterally(cacheCmd, "").trim
          Query(newQuery, Some(cacheId.trim))
        case _ => this
      }
  }

  /**
   * Extract tokens from resource pool and put them into query
   *
   * @param pool - Resource pool from interpreter context. It should contain values for tokens used in query
   * @return - New query with tokens' values embedded
   */
  private[zeppelinotl] def setTokens(pool: ResourcePool): Query = {
    case class Token(full: String, name: String)

    val newQuery: String = """\$(.*?)\$""".r.findAllIn(query).matchData
      .map(x => Token(x.group(0), x.group(1)))
      .foldLeft(query) {
        case (acc, tok) =>
          val tokenValue = Try(pool.get(tok.name).get.toString) match {
            case Success(tv) => tv
            case Failure(ex) => throw new NoSuchElementException(s"Cannot get token ${tok.name} from resource pool")
          }
          acc.replaceAllLiterally(tok.full, tokenValue)
      }
    Query(newQuery, cacheId)
  }
}

object Query {
  def apply(query: String): Query = {
    Query(query, None)
  }

  def convertToUnixTime(str: String): Int = {
    parseDate(str, "yyyy/MM/dd:hh:mm:SS") match {
      case Success(ts) => ts
      case Failure(_) => Try(str.toInt).getOrElse {
        if (str == "now") getCurrentTime else 0
      }
    }
  }

  def parseDate(str: String, format: String): Try[Int] = {
    Try((new SimpleDateFormat(format).parse(str).getTime / 1000).toInt)
  }
}
