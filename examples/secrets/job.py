from __future__ import annotations

import os
import random

from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Get the number of points from command-line arguments
    num_points = int(os.getenv("NUM_POINTS_SECRET"))

    # Create a Spark session
    spark = SparkSession.builder.appName("Pi-Estimation").getOrCreate()

    # Generate random points within the unit square
    points = spark.sparkContext.parallelize(range(1, num_points + 1)).map(
        lambda _: (random.random(), random.random())
    )

    # Count points within the unit circle
    inside_circle = points.filter(lambda point: point[0] ** 2 + point[1] ** 2 <= 1)

    # Estimate Pi
    pi_estimate = 4 * inside_circle.count() / num_points

    # Display the result
    print(f"Pi is approximately {pi_estimate}")

    # Display the second secret
    print(f"Second secret is {os.getenv('ANOTHER_SECRET')}")

    # Stop the Spark session
    spark.stop()
