package My_UsedCases;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UsedCase3 {
    long count=0;
    public static long My_func_swap1(){
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String path_orders = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_orders);
        String path_customers = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\customers\\part-00000";
        Dataset<Row> customers = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_customers);
        String path_order_items = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\order_items\\part-00000";
        Dataset<Row> order_items = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_order_items);
        orders.createOrReplaceTempView("orders");
        customers.createOrReplaceTempView("customers");
        order_items.createOrReplaceTempView("order_items");
        Dataset<Row> sqlDf = spark.sql("select c.customer_id, c.customer_fname,c.customer_lname,\n" +
                "        coalesce(round(sum(oi.order_item_subtotal),2),0) AS customer_revenue\n" +
                "        from orders o right outer join customers c\n" +
                "        on o.order_customer_id = c.customer_id\n" +
                "        join order_items oi\n" +
                "        on oi.order_item_order_id = o.order_id\n" +
                "        where o.order_date LIKE '2014-01%' and o.order_status in ('COMPLETE', 'CLOSED')\n" +
                "        group by c.customer_id, c.customer_fname, c.customer_lname\n" +
                "        order by customer_revenue desc,c.customer_id");
        return sqlDf.count();
    }
    static final Logger logger = Logger.getLogger(UsedCase3.class);
    public static void main(String[] args) {
        logger.info("*****************************************************my_spark_session started**************************************");
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String path_orders = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_orders);
        String path_customers = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\customers\\part-00000";
        Dataset<Row> customers = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_customers);
        String path_order_items = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\order_items\\part-00000";
        Dataset<Row> order_items = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_order_items);
        orders.createOrReplaceTempView("orders");
        customers.createOrReplaceTempView("customers");
        order_items.createOrReplaceTempView("order_items");
        Dataset<Row> sqlDf = spark.sql("select c.customer_id, c.customer_fname,c.customer_lname,\n" +
                "                coalesce(round(sum(oi.order_item_subtotal),2),0) AS customer_revenue\n" +
                "                from orders o right outer join customers c\n" +
                "                on o.order_customer_id = c.customer_id\n" +
                "                join order_items oi\n" +
                "                on oi.order_item_order_id = o.order_id\n" +
                "                where o.order_date LIKE '2014-01%' and o.order_status in ('COMPLETE', 'CLOSED')\n" +
                "                group by c.customer_id, c.customer_fname, c.customer_lname\n" +
                "                order by customer_revenue desc,c.customer_id");

        sqlDf.show();
        logger.info("*************************************************Showing output properly**************************************");
        String path = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\UseCaseOutput\\UsedCase3";
        sqlDf.coalesce(1).write().option("header", true).mode("overwrite").csv(path);
    }
}
