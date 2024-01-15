package com.oss_tech.examples;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class TestJob {
  public static void main(String[] args) {
    // Check if the correct number of arguments is provided
    if (args.length != 1) {
      System.err.println("Usage: PiEstimation <num_points>");
      System.exit(1);
    }

    // Initialize Spark session
    final SparkSession spark = SparkSession.builder()
        .appName("Spark Java example")
        .getOrCreate();

    // Get the number of points from command-line arguments
    final int numPoints = Integer.parseInt(args[0]);

    Dataset<Row> points = spark.range(1, numPoints + 1)
        .selectExpr("rand() as x", "rand() as y");

    // Count points within the unit circle
    long insideCircle = points.filter("x * x + y * y <= 1").count();

    // Estimate Pi
    double piEstimate = 4.0 * insideCircle / numPoints;

    // Display the result
    System.out.println("Pi is approximately " + piEstimate);
  }
}