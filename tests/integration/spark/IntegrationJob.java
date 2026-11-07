import org.apache.spark.sql.SparkSession;

public final class IntegrationJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().getOrCreate();
        try {
            if (!spark.version().equals(args[0])) {
                throw new AssertionError("Unexpected Spark runtime: " + spark.version());
            }
            long total = spark.range(100).groupBy().sum("id").first().getLong(0);
            if (total != 4950) {
                throw new AssertionError("Unexpected distributed sum: " + total);
            }
            System.out.println("SPARK_ON_K8S_JAVA_OK " + spark.version());
        } finally {
            spark.stop();
        }
    }
}
