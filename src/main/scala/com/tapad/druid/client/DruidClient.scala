package com.tapad.druid.client

import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._
import com.ning.http.client.Response

case class DruidClient(serverUrl: String) {
  import dispatch._, Defaults._
  def svc = url(serverUrl + "/druid/v2/?pretty")

  private def JsonPost(req: Req, body: String) = {
    req.POST.setHeader("Content-Type", "application/json").setBody(body)
  }

  private def parseJson(resp: Response): JValue = {
    println(resp.getResponseBody("UTF-8"))
    parse(resp.getResponseBody("UTF-8"))
  }

  private def execute[R](js: JValue, parser: JValue => R) : Future[R] = {
    println(prettyJson(js))
    Http(JsonPost(svc, prettyJson(js)) > (parseJson _).andThen(parser))
  }

  def apply(ts: TimeSeriesQuery) : Future[TimeSeriesResponse] = execute(ts.toJson, TimeSeriesResponse.parse)
  def apply(ts: GroupByQuery) : Future[GroupByResponse] = execute(ts.toJson, GroupByResponse.parse)

  def queryTimeSeries(query: String) : Future[TimeSeriesResponse] = {
    Grammar.parser.parseAll(Grammar.parser.timeSeries, query) match {
      case Grammar.parser.Success(ts, _) => apply(ts.asInstanceOf[TimeSeriesQuery])
      case failure => throw new IllegalArgumentException(failure.toString)
    }
  }

  def queryGroupBy(query: String) : Future[GroupByResponse] = {
    Grammar.parser.parseAll(Grammar.parser.groupByQuery, query) match {
      case Grammar.parser.Success(ts, _) => apply(ts.asInstanceOf[GroupByQuery])
      case failure => throw new IllegalArgumentException(failure.toString)
    }
  }

}

