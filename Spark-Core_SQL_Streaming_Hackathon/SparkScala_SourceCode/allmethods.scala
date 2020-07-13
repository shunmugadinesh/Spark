package org.inceptez.hack
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.hadoop.fs._


class allmethods {
  
  /*
  24. Custom Method creation: Create a package (org.inceptez.hack), class (allmethods),
method (remspecialchar)
Hint: First create the function directly and then later add inside pkg, class etc..
a. Method should take 1 string argument and 1 return of type string
b. Method should remove all special characters and numbers 0 to 9 - ? , / _ ( ) [ ]
Hint: Use replaceAll function, usage of [] symbol should use \\ escape sequence.
c. For eg. If I pass to the method value as Pathway - 2X (with dental) it has to
return Pathway X with dental as output.
*/
  
  // Remove all special characters and numbers 0 to 9 - ? , / _ ( ) [ ]
  def remspecialchar(value:String):String=
  {
    var modifiedValue="";
    modifiedValue = value.replaceAll("[[0-9~`\\[\\]\\\\!@#$%^&*()_+={};':\",./<>?|-]]","").replaceAll("  ", "");
    //value.replaceAll("\\W",""); to replace all special chars
    return modifiedValue
  }
  
  
  def removeHeader(rdd:RDD[String]):RDD[String]=
  {
    /*2. Remove the header line from the RDD contains column names.
			Hint: Use rdd1.first -> rdd1.filter to remove the first line identified to remove the header. */
    val header=rdd.first.toString;
    val rdd1 = rdd.filter(x=>x!=header);
    
    //3. Display the count and show few rows and check whether header is removed.
    println("Total count without header: "+rdd1.count);
    println("sample data: " + rdd1.take(3).foreach(println));
    return rdd1; 
  }

  def deleteFileIfExist(fs:FileSystem, filePaths:Array[String],isDeleteRecursively:Boolean)
  {    
    filePaths.foreach { x =>       
      var path = new Path(x);     
      if(fs.delete(path,isDeleteRecursively))
      {
        println("file exists, hence deleting. "+ x);
      }
    }
    
  } 
  
  
  
}