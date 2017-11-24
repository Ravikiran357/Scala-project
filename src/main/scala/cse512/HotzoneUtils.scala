package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // checking if point lies in the queryRectangle
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
}
