package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    // return true // YOU NEED TO CHANGE THIS PART
    var arr = queryRectangle.split(",")
    var doubleArr = arr.map(_.toDouble)
    var pointArr = pointString.split(",")
    var pointDoubleArr = pointArr.map(_.toDouble)
    if (doubleArr(0) <= pointDoubleArr(0) && doubleArr(1) <= pointDoubleArr(1) && doubleArr(2) >= pointDoubleArr(0) && doubleArr(3) >= pointDoubleArr(1)) {
      return true
    }else {
     return false
    }
  }

  // YOU NEED TO CHANGE THIS PART

}
