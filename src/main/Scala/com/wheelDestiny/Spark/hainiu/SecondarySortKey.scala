package com.wheelDestiny.Spark.hainiu

class SecondarySortKey(val word:String,val count:Int) extends Ordered[SecondarySortKey]{
  override def compare(that: SecondarySortKey): Int = {
    val i: Int = this.word.compareTo(that.word)
    if(i!=0){
      i
    }else{
      this.count-that.count
    }
  }
}

object SecondarySortKey {

}
