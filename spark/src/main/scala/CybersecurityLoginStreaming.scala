import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object CybersecurityLoginStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Cybersecurity Login Monitoring")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "cybersecurity-logins")
      .option("startingOffsets", "latest")
      .load()

    val loginSchema = new StructType()
      .add("event_id", StringType)
      .add("user_id", StringType)
      .add("location", StringType)
      .add("event_type", StringType)
      .add("timestamp", StringType)

    val parsedEvents = kafkaStream
      .selectExpr("CAST(value AS STRING) AS json")
      .select(
        from_json(col("json"), loginSchema).alias("event")
      )
      .select("event.*")

    val events = parsedEvents
      .withColumn(
        "event_time",
        to_timestamp(col("timestamp"))
      )

    val validEventTypes = Seq(
      "LOGIN_SUCCESS",
      "LOGIN_FAILED",
      "PASSWORD_RESET",
      "MFA_FAILED",
      "ACCOUNT_LOCKED"
    )

    val validEvents = events.filter(
      col("event_id").isNotNull &&
      col("user_id").isNotNull &&
      col("location").isNotNull &&
      col("event_type").isin(validEventTypes: _*) &&
      col("event_time").isNotNull
    )

    val deduplicatedEvents = validEvents
      .withWatermark("event_time", "30 seconds")
      .dropDuplicates("event_id")

    // ==================================================
    // RULE 1 + RULE 2
    // Brute-force and MFA attack detection
    // ==================================================

    val securityEvents = deduplicatedEvents
      .filter(
        col("event_type").isin(
          "LOGIN_FAILED",
          "MFA_FAILED"
        )
      )

    val securityDetections = securityEvents
      .groupBy(
        window(col("event_time"), "1 minute"),
        col("user_id"),
        col("event_type")
      )
      .count()

    val securityAlerts = securityDetections
      .filter(
        (
          col("event_type") === "LOGIN_FAILED" &&
          col("count") >= 3
        ) ||
        (
          col("event_type") === "MFA_FAILED" &&
          col("count") >= 2
        )
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("user_id"),
        col("count").alias("attempt_count"),

        when(
          col("event_type") === "LOGIN_FAILED",
          "BRUTE_FORCE_SUSPECTED"
        )
        .otherwise("MFA_ATTACK_SUSPECTED")
        .alias("alert_type"),

        lit("HIGH").alias("severity"),

        when(
          col("event_type") === "LOGIN_FAILED",
          "3 or more failed login attempts within 1 minute"
        )
        .otherwise(
          "2 or more MFA failures within 1 minute"
        )
        .alias("reason")
      )

    // ==================================================
    // RULE 3
    // Failed login followed by successful login
    // ==================================================

    val locationEvents = deduplicatedEvents
      .filter(
        col("event_type").isin(
          "LOGIN_FAILED",
          "LOGIN_SUCCESS"
        )
      )

    val locationDetection = locationEvents
      .groupBy(
        window(col("event_time"), "1 minute"),
        col("user_id")
      )
      .agg(
        sum(
          when(
            col("event_type") === "LOGIN_FAILED",
            1
          ).otherwise(0)
        ).alias("failed_count"),

        sum(
          when(
            col("event_type") === "LOGIN_SUCCESS",
            1
          ).otherwise(0)
        ).alias("success_count"),

        first(
          when(
            col("event_type") === "LOGIN_FAILED",
            col("location")
          ),
          ignoreNulls = true
        ).alias("failed_location"),

        first(
          when(
            col("event_type") === "LOGIN_SUCCESS",
            col("location")
          ),
          ignoreNulls = true
        ).alias("success_location")
      )

    val locationAlerts = locationDetection
      .filter(
        col("failed_count") >= 1 &&
        col("success_count") >= 1 &&
        col("failed_location").isNotNull &&
        col("success_location").isNotNull &&
        col("failed_location") =!= col("success_location")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("user_id"),
        lit(1).alias("attempt_count"),
        lit("SUSPICIOUS_LOCATION_CHANGE").alias("alert_type"),
        lit("MEDIUM").alias("severity"),
        concat(
          lit("Failed login from "),
          col("failed_location"),
          lit(" followed by successful login from "),
          col("success_location")
        ).alias("reason")
      )

    // ==================================================
    // COMBINE ALERTS
    // ==================================================

    val alerts = securityAlerts
      .unionByName(locationAlerts)

    // ==================================================
    // STREAM OUTPUT
    // ==================================================

    val query = alerts
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        "../checkpoints/spark-cybersecurity-security-rules-v4"
      )
      .start()

    query.awaitTermination()
  }
}
