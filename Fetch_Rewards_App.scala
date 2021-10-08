import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ 
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.pyspark.ml.feature.VectorAssembler
import org.apache.pyspark.ml.feature.StringIndexer

object Main {
  val spark = SparkSession.builder().appName("Spark SQL basic Practice").config("spark.some.config.option", "some-value").getOrCreate()
  
  /*
   Purpose: Comapre the usage of DIGITAL RECEIPT vs non DIGITAL RECEIPT.
   Input: The format of the dataframe to return which is either csv, JSON, or Parquet.
   Output: A tuple where index 0 is a percentage value for DIGITAL RECEIPT used and index 1 is a percentage value for non DIGITAL RECEIPT used.
*/
  def analysis1(month: Int): (Int, Int) = {
    var df_v3: DataFrame = spark.read.csv("/content/rewards_receipts_lat_v3.csv", header=true, inferSchema=true);
    var subset: DataFrame = df_v3.filter(split(col("RECEIPT_PURCHASE_DATE"), "-")(1) <= month);
    var count: Int = subset.select("DIGITAL_RECEIPT").count();
    var df_true: Int = subset.filter(("DIGITAL_RECEIPT == true"));
    var count_true: Double = df_true.count()/count;
    var df_false: Int = subset.filter("DIGITAL_RECEIPT == false");
    var count_false: Double = df_false.count()/count;
    var results: (Int, Int) = (count_true, count_false);
    return results;
  }

  /*
   Purpose: To see which states are the highest purchase tickets coming from and therefore are the most profitable.
   Input: A dataframe.
   Outout: A dataframe grouped by state and ordered from highest ticket purchases to the lowest ones.
*/
  def analysis2(format: String): DataFrame = {
     var df_v3: DataFrame = spark.read.csv("/content/rewards_receipts_lat_v3.csv", header=true, inferSchema=true);
     var store_state: DataFrame = df_v3.groupBy("STORE_STATE").sum("RECEIPT_TOTAL").orderBy("sum(RECEIPT_TOTAL)", ascending=false).show();

    if (format == "csv") {
      return store_state;
    }
    if (format == "JSON") {
      store_state.write.format("json").mode("overwrite").save(new_storeState);
      return new_storeState;
    }
    else {
      store_state.write.format("parquet").mode("overwrite").save(new_storeState);
      return new_storeState;
    }
  }

  /*
   Purpose: To train a Logistic Regression model to predict product brand name.
   Input: A dataframe.
   output: An evaluation of the Logistic Regression model.
*/
  def analysis3(): Unit = {
     //Clean the data.
    var df_v2: DataFrame = spark.read.csv("/content/rewards_receipts_item_lat_v2.csv", header=true, inferSchema=true);
    var df_v2: DataFrame = df_v2.na.drop(how="any", subset=("ITEM_PRICE", "WEIGHT", "PRODUCT_NAME", "CATEGORY");
    //Create feature vector.
    var indexer: StringIndexer = new StringIndexer(inputCols=("CATEGORY", "PRODUCT_NAME"), outputCols=("CATEGORY_INDEXED", "PRODUCT_NAME_INDEXED");
    var df: DataFrame = indexer.fit(df_v2).transform(df_v2);
    var featureVector: VectorAssembler = new VectorAssembler(inputCols=("WEIGHT", "ITEM_PRICE", "CATEGORY_INDEXED"), outputCol="Independent Features");
    var new_df: DataFrame = featureVector.transform(df);
    //create an instance of the model and train it.
    var train_data, test_data = new_df.select(("Independent Features", "PRODUCT_NAME_INDEXED").randomSplit((0.75, 0.25);
    var regressor: LogisticRegression = new LogisticRegression(labelCol="PRODUCT_NAME_INDEXED",featuresCol="Independent Features", maxIter=10, family="multinomial");
    regressor = regressor.fit(train_data);
    var results = regressor.evaluate(test_data);
    results.predictions.show();

  }

  // function that will execute the application.
  def application(analysis: String, month: Int, format: String): Unit = {
    if (analysis == "analysis1") {
      println(analysis1(month));
    }
    if (analysis == "analysis2") {
      println(analysis2(format));
    }
    else {
      println(analysis3());
    }
  }

  def main(args: Array[String]): Unit = {
    val analysis: String = "analysis1";
    val month: Int = 2;
    val format: String = "csv";
    println(application(analysis, month, format));
  }
}
