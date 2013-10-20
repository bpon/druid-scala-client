package com.tapad.druid.client

import org.scalatest._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Interval}

class GrammarSpec extends FlatSpec with Matchers {

  import Grammar._
  def date(s: String) = DateTimeFormat.forPattern("YYYY-MM-dd").parseDateTime(s)

  def expr(s: String): Expression = parser.parseAll(parser.filterExpression, s) match {
    case parser.Success(r, _) => r.asInstanceOf[Expression]
    case x => fail(x.toString)
  }

  "The expression parser" should "parse simple numeric filter expressions" in {
    expr("age = 9000") should be(SelectorFilter("age", "9000"))
  }
  it should "parse simple string double quote filter expressions" in {
    expr( """name = "Druid"""") should be(SelectorFilter("name", "Druid"))
  }
  it should "parse simple string single quote filter expressions" in {
    expr( """name = 'Druid'""") should be(SelectorFilter("name", "Druid"))
  }
  it should "parse escaped string filter expressions" in {
    expr( """name = "Druid has a ""name"" " """) should be(SelectorFilter("name", """Druid has a "name" """))
  }
  it should "parse nested expressions" in {
    expr( """name = "Druid" and age = 9000 or age = 21""") should be(
      Or(Seq(
        And(
          Seq(
            SelectorFilter("name", "Druid"),
            SelectorFilter("age", "9000")
          )),
        SelectorFilter("age", "21")
      ))
    )
  }
  it should "parse honor parens in expressions" in {
    val res = expr( """(name = "Druid") and (age = 9000 or (age = 21 and level = "VIP"))""")
    res should be(
      And(Seq(
        SelectorFilter("name", "Druid"),
        Or(
          Seq(
            SelectorFilter("age", "9000"),
            And(Seq(
              SelectorFilter("age", "21"),
              SelectorFilter("level", "VIP")
            ))
          )
        )))
    )
  }


  "The query parser" should "parse time series expressions" in {
    def ts(s: String): TimeSeriesQuery = parser.parseAll(parser.timeSeries, s) match {
      case parser.Success(r, _) => r.asInstanceOf[TimeSeriesQuery]
      case x => fail(x.toString)
    }
    ts("hourly between '2013-01-01' and '2013-01-31' select longSum(users), longSum(pageViews) as pages from users where age = 50") should be (
      TimeSeriesQuery(
        source = "users",
        interval = new Interval(date("2013-01-01"), date("2013-01-31")),
        granularity = Granularity.Hour,
        aggregate = Seq(
          Aggregation("longSum", "users", "users"),
          Aggregation("longSum", "pageViews", "pages")
        ),
        postAggregate = Nil,
        filter = Filter.where("age", "50")
      )
    )
    ts("daily (between '2013-01-01 00:00' and '2013-01-31 16:00') select longSum(users), longSum(pageViews) as pages from users where age = 50") should be (
      TimeSeriesQuery(
        source = "users",
        interval = new Interval(date("2013-01-01"), date("2013-01-31").plusHours(16)),
        granularity = Granularity.Day,
        aggregate = Seq(
          Aggregation("longSum", "users", "users"),
          Aggregation("longSum", "pageViews", "pages")
        ),
        postAggregate = Nil,
        filter = Filter.where("age", "50")
      )
    )
  }

}