package My_UsedCases;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UsedCase4 {
    long count=0;
    public static long My_func_swap2(){
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String path_orders = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_orders);

        String path_order_items = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\order_items\\part-00000";
        Dataset<Row> order_items = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_order_items);

        String path_products = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\products\\part-00000";
        Dataset<Row> products = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_products);

        String path_categories = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\categories\\part-00000";
        Dataset<Row> categories = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_categories);
        orders.createOrReplaceTempView("orders");
        order_items.createOrReplaceTempView("order_items");
        products.createOrReplaceTempView("products");
        categories.createOrReplaceTempView("categories");
        Dataset<Row> sqlDf = spark.sql("SELECT c.*, round(sum(oi.order_item_subtotal), 2) as category_revenue\n" +
                "FROM orders o join order_items oi\n" +
                "     on o.order_id = oi.order_item_order_id\n" +
                "     join products p \n" +
                "     on p.product_id = oi.order_item_product_id\n" +
                "     join categories c\n" +
                "     on c.category_id = p.product_category_id\n" +
                "WHERE date_format(o.order_date, 'yyyy-MM') LIKE '2014-01%' and o.order_status IN('COMPLETE','CLOSED')\n" +
                "group by c.category_id,c.category_department_id,c.category_name\n" +
                "order by c.category_id");
        return sqlDf.count();
    }
    static final Logger logger = Logger.getLogger(UsedCase4.class);
    public static void main(String[] args) {
        logger.info("*****************************************************my_spark_session started**************************************");
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String path_orders = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_orders);

        String path_order_items = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\order_items\\part-00000";
        Dataset<Row> order_items = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_order_items);

        String path_products = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\products\\part-00000";
        Dataset<Row> products = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_products);

        String path_categories = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\categories\\part-00000";
        Dataset<Row> categories = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_categories);
        orders.createOrReplaceTempView("orders");
        order_items.createOrReplaceTempView("order_items");
        products.createOrReplaceTempView("products");
        categories.createOrReplaceTempView("categories");
        Dataset<Row> sqlDf = spark.sql("SELECT c.*, round(sum(oi.order_item_subtotal), 2) as category_revenue\n" +
                "FROM orders o join order_items oi\n" +
                "     on o.order_id = oi.order_item_order_id\n" +
                "     join products p \n" +
                "     on p.product_id = oi.order_item_product_id\n" +
                "     join categories c\n" +
                "     on c.category_id = p.product_category_id\n" +
                "WHERE date_format(o.order_date, 'yyyy-MM') LIKE '2014-01%' and o.order_status IN('COMPLETE','CLOSED')\n" +
                "group by c.category_id,c.category_department_id,c.category_name\n" +
                "order by c.category_id");
        sqlDf.show();
        logger.info("*************************************************Showing output properly**************************************");
        String path = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\UseCaseOutput\\UsedCase4";
        sqlDf.coalesce(1).write().option("header", true).mode("overwrite").csv(path);
    }
}