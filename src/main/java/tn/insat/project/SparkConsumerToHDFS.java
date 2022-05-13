package tn.insat.project;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkConsumerToHDFS {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        String day = java.time.LocalDate.now().toString();
        
        if(args.length == 0){
            System.out.println("Topic name ? :");
            return;
        }
        String topicName = args[0];

        SparkSession sparkSession = SparkSession.builder()
                .appName("store").getOrCreate();


        Dataset<Row> loadDS = sparkSession.read().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", topicName).load().selectExpr("CAST(value as STRING)");

        //loadDS.show();

        StructType  schema = new  StructType ()
                .add("userID", DataTypes.IntegerType)
                .add("product", DataTypes.StringType)
                .add("price", DataTypes.IntegerType)
                .add("time", DataTypes.TimestampType);


        Dataset<Row> rawDS = loadDS.select(functions.from_json(loadDS.col("value"), schema).as("data")).select("data.*");

        /*
        rawDS.show(5, false);
        +------+---------+-----+----------------------+
        |userID| product |price|       time           |
        +------+---------+-----+----------------------+
        |79    |T-shirt  |79   |2022-05-12 10:56:28   |
        |1240  |jeans    |189  |2022-05-12 10:15:52   |
        |101   |socks    |183  |2022-05-12 00:45:14   |
        |489   |jacket   |334  |2022-05-12 05:44:32   |
        |2     |T-shirt  |105  |2022-05-12 19:41:02   |
        +------+---------+-----+------------+---------+
        */


        // ---------- TOP SELLING PRODUCTS--------------
        Dataset<Row> topSellingProducts = rawDS.groupBy(rawDS.col("product")).count().sort(functions.desc("count"));

        /*
        topSellingProducts.show(4 , false);
        +------------+-----+
        |product     |count|
        +------------+-----+
        |jeans       |1051 |
        |coat        |990  |
        |socks       |977  |
        |sweater     |958  |
        +------------+-----+
         */

        //-----------NUMBER OF ITEMS SOLD BY HOURS OF THE DAY-------------

        Dataset<Row> numberOfItemsSoldPerHour = rawDS.groupBy(functions.window(rawDS.col("time"), "1 hour"), rawDS.col("product")).count()
                .groupBy("window").pivot("product").sum("count");

        /*
        numberOfItemsSoldPerHour.show (5, false);
        +------------------------------------------+-------+-------+------+----+-----+-----+----- +
        |                   window                 |T-shirt|sweater|jacket|coat|jeans|socks|shorts|
        +------------------------------------------+-------+-------+------+----+-----+-----+------+
        |[2022-05-12 20:00:00, 2022-05-12 21:00:00]|   50  |  50   |  39  | 36 | 39  | 39  | 33   |
        |[2022-05-12 21:00:00, 2022-05-12 22:00:00]|   36  |  43   |  42  | 41 | 58  | 39  | 39   |
        |[2022-05-12 06:00:00, 2022-05-12 07:00:00]|   44  |  40   |  41  | 35 | 43  | 36  | 38   |
        |[2022-05-12 11:00:00, 2022-05-12 12:00:00]|   43  |  47   |  37  | 41 | 56  | 40  | 33   |
        |[2022-05-12 03:00:00, 2022-05-12 04:00:00]|   40  |  31   |  29  | 40 | 40  | 47  | 32   |
        +------------------------------------------+-------+-------+------+----+-----+-----+------+
         */

        //-------------THE BUSIEST HOURS OF THE DAY-------------

        Dataset<Row> highestHoursOfTheDay = rawDS.groupBy(functions.window(rawDS.col("time"), "1 hour"), rawDS.col("userID")).count()
                .groupBy("window").sum("count").sort(functions.desc("sum(count)"));

        Dataset < Row > busiestHoursOfTheDay_final =  highestHoursOfTheDay.withColumn ( "totalCustomers" , highestHoursOfTheDay.col ( "sum(count)" )). drop ( highestHoursOfTheDay.col ( "sum(count)" ));

        /*
        busiestHoursOfTheDay_final.show(5 , false);
        +------------------------------------------+-------------+
        |window                                    |totalCustomers|
        +------------------------------------------+-------------+
        |[2022-05-12 19:00:00, 2022-05-12 20:00:00]|708          |
        |[2022-05-12 21:00:00, 2022-05-12 22:00:00]|706          |
        |[2022-05-12 11:00:00, 2022-05-12 12:00:00]|701          |
        |[2022-05-12 09:00:00, 2022-05-12 10:00:00]|701          |
        |[2022-05-12 06:00:00, 2022-05-12 07:00:00]|690          |
        +------------------------------------------+-------------+
         */


        //-------------- CUSTOMERS TAKING THE MOST PRODUCTS AND AMOUNTS PAYED ----------

        Dataset<Row> priceForBuyingTheMostProducts = rawDS.groupBy("userID", "product", "price").count()
                .groupBy("userID").sum("count", "price").sort(functions.desc("sum(count)"));


        Dataset<Row> priceForBuyingTheMostProducts_final= priceForBuyingTheMostProducts.withColumnRenamed("sum(count)", "unit").withColumnRenamed("sum(price)", "amount");

        /*
        priceForBuyingTheMostProducts.show(5 , false);
        +------+----+-----+
        |userID|unit|amount|
        +------+----+-----+
        |1030  |18  |4520 |
        |1799  |18  |3426 |
        |1581  |17  |3537 |
        |511   |17  |2723 |
        |358   |17  |2735 |
        +------+----+-----+
         */




        //-------SAVE HDFS---------------

        /*
         numPartitions = 1 => All the data will be saved in a single output file,

         first fetched to a single worker and subsequently distributed over storage nodes.
        **/

        topSellingProducts.coalesce(1).write().csv("hdfs://localhost:8088/data/"+day);

        numberOfItemsSoldPerHour.coalesce(1).write().csv("hdfs://localhost:8088/data/"+day);

        busiestHoursOfTheDay_final.coalesce( 1 ).write().csv( "hdfs://localhost:8088/data/"+day);

        priceForBuyingTheMostProducts_final.coalesce(1).write().csv("hdfs://localhost:8088/data/"+day);


    }
}
