package com.tapad.druid.client

import scala.concurrent.ExecutionContext
import org.joda.time.{DateTime, Interval}
import scala.util.{Failure, Success}

object WikipediaExample {

  def main(args: Array[String]) {
    implicit val executionContext = ExecutionContext.Implicits.global
    val client = DruidClient("http://localhost:8083")

    import com.tapad.druid.client.DSL._
    val query = GroupByQuery(
      source = "wikipedia",
      interval = new Interval(new DateTime().minusDays(1), new DateTime()),
      dimensions = Seq("page"),
      granularity = Granularity.All,
      aggregate = Seq(
        sum("count") as "edits",
        sum("added") as "chars_added"
      ),
      postAggregate = Seq(
        "chars_added" / "edits" as "chars_per_edit"
      ),
      filter = "namespace" === "article" and "country" === "United States",
      orderBy = Seq(
        "chars_added" desc
      ),
      limit = Some(100)
    )

    client(query).onComplete {
      case Success(resp) =>
        resp.data.foreach { row =>
          println("Page %s, %s edits, %s chars added, %s per edit".format(
            row("page"), row("edits"), row("chars_added"), row("chars_per_edit")
          ))
        }
        System.exit(0)
      case Failure(ex) =>
        ex.printStackTrace()
        System.exit(0)
    }

  }
}
