package Exp2

import java.util.Calendar
import org.apache.commons.lang3.time.FastDateFormat

class NewDate(year: Int, month: Int, day: Int) extends Date(year, month, day) {
  var dayOfWeek = -1

  def this(year: Int) {
    this(year, -1, -1)
  }

  def this(year: Int, month: Int) {
    this(year, month, -1)
  }

  def this(year: Int, month: Int, day: Int, dayOfWeek: Int) {
    this(year, month, day)
    this.dayOfWeek = dayOfWeek
  }

  def dateToWeekDay(): Int = {
    val yearString = this.year.toString
    var monthString = ""
    var dayString = ""
    if (this.month % 10 == this.month) {
      monthString = "0" + this.month.toString
    }
    else {
      monthString = this.month.toString
    }
    if (this.day % 10 == this.day) {
      dayString = "0" + this.day
    }
    else {
      dayString = this.day.toString
    }
    val standardTime = yearString + monthString + dayString
    val dateTime = FastDateFormat.getInstance("yyyyMMdd").parse(standardTime)
    val cal = Calendar.getInstance()
    cal.setTime(dateTime)
    this.dayOfWeek = cal.get(Calendar.DAY_OF_WEEK) - 1
    return this.dayOfWeek
  }
}

object Question2 {
  def main(args: Array[String]): Unit = {
    var newDate = new NewDate(2022, 1, 4)
    var newDate2 = new NewDate(2022, 1, 5)
    newDate.dateToWeekDay()
    println("The day of week of newDate: " + newDate.dayOfWeek)
    println("newDate compared to newDate2: " + newDate.before(newDate2))
  }
}

