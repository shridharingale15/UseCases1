package My_UsedCases;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UsedCase5 {
    long count=0;
    public static long My_func_swap6(){
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String path_departments = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\departments\\part-00000";
        Dataset<Row> departments = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_departments);

        String path_products = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\products\\part-00000";
        Dataset<Row> products = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_products);

        String path_categories = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\categories\\part-00000";
        Dataset<Row> categories = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_categories);
        departments.createOrReplaceTempView("departments");
        products.createOrReplaceTempView("products");
        categories.createOrReplaceTempView("categories");
        Dataset<Row> sqlDf = spark.sql("SELECT d.department_id,d.department_name,count(*) as product_count\n" +
                "from departments d join categories c\n" +
                "     on d.department_id=c.category_department_id\n" +
                "     join products p\n" +
                "     on p.product_category_id=c.category_id\n" +
                "WHERE p.product_category_id=c.category_id\n" +
                "group by d.department_id,d.department_name\n" +
                "order by  d.department_id");
        return sqlDf.count();
    }
    static final Logger logger = Logger.getLogger(UsedCase5.class);
    public static void main(String[] args) {
        logger.info("*****************************************************my_spark_session started**************************************");
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String path_departments = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\departments\\part-00000";
        Dataset<Row> departments = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_departments);

        String path_products = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\products\\part-00000";
        Dataset<Row> products = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_products);

        String path_categories = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\resources\\retail_db\\categories\\part-00000";
        Dataset<Row> categories = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_categories);
        departments.createOrReplaceTempView("departments");
        products.createOrReplaceTempView("products");
        categories.createOrReplaceTempView("categories");
        Dataset<Row> sqlDf = spark.sql("SELECT d.department_id,d.department_name,count(*) as product_count\n" +
                "from departments d join categories c\n" +
                "     on d.department_id=c.category_department_id\n" +
                "     join products p\n" +
                "     on p.product_category_id=c.category_id\n" +
                "WHERE p.product_category_id=c.category_id\n" +
                "group by d.department_id,d.department_name\n" +
                "order by  d.department_id");
        sqlDf.show();
        logger.info("*************************************************Showing output properly**************************************");
        String path = "C:\\Users\\Shridhar Ingale\\IdeaProjects\\UseCases1\\src\\main\\UseCaseOutput\\UsedCase5";
        sqlDf.coalesce(1).write().option("header", true).mode("overwrite").csv(path);
    }
}