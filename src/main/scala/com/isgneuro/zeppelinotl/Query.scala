package com.isgneuro.zeppelinotl

import com.isgneuro.otp.connector.utils.Datetime.getCurrentTime
import org.apache.zeppelin.resource.ResourcePool

import java.text.SimpleDateFormat
import scala.util.{ Failure, Success, Try }

case class Query(query: String) {

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
            case Failure(ex) => throw new NoSuchElementException(s"Caught exception '${ex.getMessage}', Cannot get token ${tok.name} from resource pool")
          }
          acc.replaceAllLiterally(tok.full, tokenValue)
      }
    Query(newQuery)
  }
}

