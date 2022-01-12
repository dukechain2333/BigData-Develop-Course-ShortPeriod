package Exp2

class Date() {
  private var year = -1
  private var month = -1
  private var day = -1
  private var dateLength = 0

  def this(year: Int) {
    this()
    this.year = year
    this.dateLength = 1
  }

  def this(year: Int, month: Int) {
    this(year)
    this.month = month
    this.dateLength = 2
  }

  def this(year: Int, month: Int, day: Int) {
    this(year, month)
    this.day = day
    this.dateLength = 3
  }

  def before(date: Date): Boolean = {
    if (this.dateLength == date.dateLength & this.year != -1) {

      if (this.dateLength == 1) {
        if (this.year < date.year) {
          return true
        }
        else return false
      }

      else if (this.dateLength == 2) {
        if (this.year > date.year) {
          return false
        }
        else if (this.year < date.year) {
          return true
        }
        else {
          if (this.month > date.month) {
            return false
          }
          else return true
        }
      }

      else if (this.dateLength == 3) {
        if (this.year > date.year) {
          return false
        }
        else if (this.year < date.year) {
          return true
        }
        else {
          if (this.month > date.month) {
            return false
          }
          else if (this.month < date.month) {
            return true
          }
          else {
            if (this.day >= date.day) {
              return false
            }
            else {
              return true
            }
          }
        }
      }
      return false
    }
    else {
      return false
    }
  }
}

object Question1 {
  def main(args: Array[String]): Unit = {
    val date1 = new Date()
    val date2 = new Date(2022)
    val date3 = new Date(2022, 1)
    val date4 = new Date(2022, 1, 5)

    val date5 = new Date()
    val date6 = new Date(2021)
    val date7 = new Date(2023, 1)
    val date8 = new Date(2022, 1, 4)

    println("Date1 and Date5: " + date1.before(date5))
    println("Date2 and Date6: " + date2.before(date6))
    println("Date3 and Date7: " + date3.before(date7))
    println("Date4 and Date8: " + date4.before(date8))
    println("Date1 and Date2: " + date1.before(date2))
  }
}
