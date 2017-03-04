import java.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

/**
  * Created by mustafamuswadi on 2/26/17.
  */
object TweetOrganization {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("Directory Creator").setMaster("local")

    val sc = new SparkContext(conf)


    //Reading the input file that contains tweets
    val inputFile = sc.textFile("file://"+args(0))

    //Reading each tweet in the file
    val hashTagsRDD=  inputFile.map(line => {

      var hashTagList = List[String]()

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      //Checking wheather tweets contains hashtags or not if it contains more than 0

      //extracting each and every hashtag from tweet
      for(i <- 0 to hashTags.length()-1){
        val tempObject = hashTags.getJSONObject(i)

        hashTagList = hashTagList.::(tempObject.getString("text"))
      }

      hashTagList
    })

    val hashTagEntities = hashTagsRDD.filter(line => line.length >0).flatMap(line => line)

    val topNHashTags = hashTagEntities.map(word => (word.toLowerCase,1)).reduceByKey(_+_).sortBy(line => line._2,false).map(line => line._1).collect.take(10)



    //None
    val noneHashTags = inputFile.map(line => {

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      //Checking wheather tweets contains hashtags or not if it contains more than 0
      if(hashTags.length() > 0){
        "none"
      }else{
        line.trim

      }
    }).filter(line => !line.equalsIgnoreCase("none")).saveAsTextFile(args(1)+"/none")



    val top1HashTags = inputFile.map(line => {

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      var status = false

      //Checking wheather tweets contains hashtags or not if it contains more than 0
      if(hashTags.length() > 0){

        //extracting each and every hashtag from tweet
        for(i <- 0 to hashTags.length()-1){

          val tempObject = hashTags.getJSONObject(i)

          val tempHashTag = tempObject.getString("text")

          if(topNHashTags.contains(tempHashTag.toLowerCase)){
            if(tempHashTag.equalsIgnoreCase(topNHashTags(0))){
              status =true
            }
          }else{

          }

        }

      }else{

      }

      if(status){
        line
      }else{
        "none"
      }

    }).filter(line => !line.equalsIgnoreCase("none")).saveAsObjectFile(args(1)+"/"+topNHashTags(0))


    //Second top

    val top2HashTags = inputFile.map(line => {

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      var status = false

      //Checking wheather tweets contains hashtags or not if it contains more than 0
      if(hashTags.length() > 0){

        //extracting each and every hashtag from tweet
        for(i <- 0 to hashTags.length()-1){

          val tempObject = hashTags.getJSONObject(i)

          val tempHashTag = tempObject.getString("text")

          if(topNHashTags.contains(tempHashTag.toLowerCase)){
            if(tempHashTag.equalsIgnoreCase(topNHashTags(1))){
              status =true
            }
          }else{

          }

        }

      }else{

      }

      if(status){
        line
      }else{
        "none"
      }

    }).filter(line => !line.equalsIgnoreCase("none")).saveAsObjectFile(args(1)+"/"+topNHashTags(1))

    //third top



    val top3HashTags = inputFile.map(line => {

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      var status = false

      //Checking wheather tweets contains hashtags or not if it contains more than 0
      if(hashTags.length() > 0){

        //extracting each and every hashtag from tweet
        for(i <- 0 to hashTags.length()-1){

          val tempObject = hashTags.getJSONObject(i)

          val tempHashTag = tempObject.getString("text")

          if(topNHashTags.contains(tempHashTag.toLowerCase)){
            if(tempHashTag.equalsIgnoreCase(topNHashTags(2))){
              status =true
            }
          }else{

          }

        }

      }else{

      }

      if(status){
        line
      }else{
        "none"
      }

    }).filter(line => !line.equalsIgnoreCase("none")).saveAsObjectFile(args(1)+"/"+topNHashTags(2))


    //Top 4hashtgas
    val top4HashTags = inputFile.map(line => {

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      var status = false

      //Checking wheather tweets contains hashtags or not if it contains more than 0
      if(hashTags.length() > 0){

        //extracting each and every hashtag from tweet
        for(i <- 0 to hashTags.length()-1){

          val tempObject = hashTags.getJSONObject(i)

          val tempHashTag = tempObject.getString("text")

          if(topNHashTags.contains(tempHashTag.toLowerCase)){
            if(tempHashTag.equalsIgnoreCase(topNHashTags(3))){
              status =true
            }
          }else{

          }

        }

      }else{

      }

      if(status){
        line
      }else{
        "none"
      }

    }).filter(line => !line.equalsIgnoreCase("none")).saveAsObjectFile(args(1)+"/"+topNHashTags(3))


    //Top 5hashtgas
    val top5HashTags = inputFile.map(line => {

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      var status = false

      //Checking wheather tweets contains hashtags or not if it contains more than 0
      if(hashTags.length() > 0){

        //extracting each and every hashtag from tweet
        for(i <- 0 to hashTags.length()-1){

          val tempObject = hashTags.getJSONObject(i)

          val tempHashTag = tempObject.getString("text")

          if(topNHashTags.contains(tempHashTag.toLowerCase)){
            if(tempHashTag.equalsIgnoreCase(topNHashTags(4))){
              status =true
            }
          }else{

          }

        }

      }else{

      }

      if(status){
        line
      }else{
        "none"
      }

    }).filter(line => !line.equalsIgnoreCase("none")).saveAsObjectFile(args(1)+"/"+topNHashTags(4))

    //Top 6hashtgas
    val top6HashTags = inputFile.map(line => {

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      var status = false

      //Checking wheather tweets contains hashtags or not if it contains more than 0
      if(hashTags.length() > 0){

        //extracting each and every hashtag from tweet
        for(i <- 0 to hashTags.length()-1){

          val tempObject = hashTags.getJSONObject(i)

          val tempHashTag = tempObject.getString("text")

          if(topNHashTags.contains(tempHashTag.toLowerCase)){
            if(tempHashTag.equalsIgnoreCase(topNHashTags(5))){
              status =true
            }
          }else{

          }

        }

      }else{

      }

      if(status){
        line
      }else{
        "none"
      }

    }).filter(line => !line.equalsIgnoreCase("none")).saveAsObjectFile(args(1)+"/"+topNHashTags(5))


    //Top 7hashtgas
    val top7HashTags = inputFile.map(line => {

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      var status = false

      //Checking wheather tweets contains hashtags or not if it contains more than 0
      if(hashTags.length() > 0){

        //extracting each and every hashtag from tweet
        for(i <- 0 to hashTags.length()-1){

          val tempObject = hashTags.getJSONObject(i)

          val tempHashTag = tempObject.getString("text")

          if(topNHashTags.contains(tempHashTag.toLowerCase)){
            if(tempHashTag.equalsIgnoreCase(topNHashTags(6))){
              status =true
            }
          }else{

          }

        }

      }else{

      }

      if(status){
        line
      }else{
        "none"
      }

    }).filter(line => !line.equalsIgnoreCase("none")).saveAsObjectFile(args(1)+"/"+topNHashTags(6))


    //Top 7 hashtgas
    val top8HashTags = inputFile.map(line => {

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      var status = false

      //Checking wheather tweets contains hashtags or not if it contains more than 0
      if(hashTags.length() > 0){

        //extracting each and every hashtag from tweet
        for(i <- 0 to hashTags.length()-1){

          val tempObject = hashTags.getJSONObject(i)

          val tempHashTag = tempObject.getString("text")

          if(topNHashTags.contains(tempHashTag.toLowerCase)){
            if(tempHashTag.equalsIgnoreCase(topNHashTags(7))){
              status =true
            }
          }else{

          }

        }

      }else{

      }

      if(status){
        line
      }else{
        "none"
      }

    }).filter(line => !line.equalsIgnoreCase("none")).saveAsObjectFile(args(1)+"/"+topNHashTags(7))



    //Top 9hashtgas
    val top9HashTags = inputFile.map(line => {

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      var status = false

      //Checking wheather tweets contains hashtags or not if it contains more than 0
      if(hashTags.length() > 0){

        //extracting each and every hashtag from tweet
        for(i <- 0 to hashTags.length()-1){

          val tempObject = hashTags.getJSONObject(i)

          val tempHashTag = tempObject.getString("text")

          if(topNHashTags.contains(tempHashTag.toLowerCase)){
            if(tempHashTag.equalsIgnoreCase(topNHashTags(8))){
              status =true
            }
          }else{

          }

        }

      }else{

      }

      if(status){
        line
      }else{
        "none"
      }

    }).filter(line => !line.equalsIgnoreCase("none")).saveAsObjectFile(args(1)+"/"+topNHashTags(8))


    //Top 10hashtgas
    val top10HashTags = inputFile.map(line => {

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      var status = false

      //Checking wheather tweets contains hashtags or not if it contains more than 0
      if(hashTags.length() > 0){

        //extracting each and every hashtag from tweet
        for(i <- 0 to hashTags.length()-1){

          val tempObject = hashTags.getJSONObject(i)

          val tempHashTag = tempObject.getString("text")

          if(topNHashTags.contains(tempHashTag.toLowerCase)){
            if(tempHashTag.equalsIgnoreCase(topNHashTags(9))){
              status =true
            }
          }else{

          }

        }

      }else{

      }

      if(status){
        line
      }else{
        "none"
      }

    }).filter(line => !line.equalsIgnoreCase("none")).saveAsObjectFile(args(1)+"/"+topNHashTags(9))

    //Others
    val othersHashTags = inputFile.map(line => {

      //Soring each tweet into temporary variable called 'temp' for further usage
      val temp = line

      //Converting line to JSONObject to access hashtags from original tweet
      val jsonObject = new JSONObject(temp)

      //Since hashtags are inside of entities so we need to extract entities and then we have to extract hashtags from entities
      //So extracting entities from tweet
      val entities = jsonObject.getJSONObject("entities")

      //extracting hashtags from entities received above
      val hashTags = entities.getJSONArray("hashtags")

      var status = false

      //Checking wheather tweets contains hashtags or not if it contains more than 0
      if(hashTags.length() > 0){

        //extracting each and every hashtag from tweet
        for(i <- 0 to hashTags.length()-1){

          val tempObject = hashTags.getJSONObject(i)

          val tempHashTag = tempObject.getString("text")

          if(topNHashTags.contains(tempHashTag.toLowerCase)){
            status = true
          }

        }

      }//None group
      else{

      }

      if(status){
        "none"
      }else{
        line
      }

    }).filter(line => !line.equalsIgnoreCase("none")).saveAsObjectFile(args(1)+"/others")

  }
}