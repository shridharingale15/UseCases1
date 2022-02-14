package My_UsedCases;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;

public class UsedCase1 {
    long count=0;
    public static long My_func(){
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String path_orders = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_orders);
        String path_customers = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\customers\\part-00000";
        Dataset<Row> customers = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_customers);
        orders.createOrReplaceTempView("orders");
        customers.createOrReplaceTempView("customers");
        Dataset<Row> sqlDf = spark.sql("SELECT c.customer_id,c.customer_fname,c.customer_lname,count(c.customer_id) as customer_order_count \n" +
                "FROM orders o join customers c\n" +
                "ON o.order_customer_id=c.customer_id\n" +
                "WHERE  date_format(o.order_date, 'yyyy-MM-dd') LIKE '2014-01%'\n" +
                "GROUP BY c.customer_id,c.customer_fname,c.customer_lname\n" +
                "ORDER BY customer_order_count DESC,c.customer_id");
        return sqlDf.count();

    }
    static final Logger logger = Logger.getLogger(UsedCase1.class);

    public static void main(String[] args) {
        logger.info("*****************************************************my_spark_session started**************************************");
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String path_orders = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_orders);
        String path_customers = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\customers\\part-00000";
        Dataset<Row> customers = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_customers);
        orders.createOrReplaceTempView("orders");
        customers.createOrReplaceTempView("customers");
        Dataset<Row> sqlDf = spark.sql("SELECT c.customer_id,c.customer_fname,c.customer_lname,count(c.customer_id) as customer_order_count \n" +
                "FROM orders o join customers c\n" +
                "ON o.order_customer_id=c.customer_id\n" +
                "WHERE  date_format(o.order_date, 'yyyy-MM-dd') LIKE '2014-01%'\n" +
                "GROUP BY c.customer_id,c.customer_fname,c.customer_lname\n" +
                "ORDER BY customer_order_count DESC,c.customer_id");

        sqlDf.show();
        logger.info("*************************************************Showing output properly**************************************");
        String path = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\UseCaseOutput\\UsedCase1";
        sqlDf.coalesce(1).write().option("header", true).mode("overwrite").csv(path);
    }
}
