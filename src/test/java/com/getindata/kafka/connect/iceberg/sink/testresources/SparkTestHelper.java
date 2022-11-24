package com.getindata.kafka.connect.iceberg.sink.testresources;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;

import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.*;

public class SparkTestHelper {
    private final SparkSession spark;

    private SparkTestHelper(SparkSession spark) {
        this.spark = spark;
    }

    public static String getLocalCatalogTableName(String table) {
        return "local." + table;
    }

    public Dataset<Row> query(String stmt) {
        return spark.newSession().sql(stmt);
    }

    public Dataset<Row> table(String table) {
        return spark.newSession().table(table);
    }

    public Dataset<Row> table(String table, long snapshotId) {
        return spark.newSession().read().option("snapshot-id", snapshotId).table(table);
    }

    public void shutdownSession() {
        this.spark.stop();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        SparkConf sparkconf;

        private Builder() {
            sparkconf = new SparkConf();

            sparkconf.setAppName("CDC-S3-Batch-Spark-Sink")
                    .setMaster("local[2]")
                    .set("spark.ui.enabled", "false")
                    .set("spark.eventLog.enabled", "false")
                    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        }

        public Builder withLocalCatalog(Path localWarehouseDir) {
            sparkconf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                    .set("spark.sql.catalog.local.type", "hadoop")
                    .set("spark.sql.catalog.local.warehouse", localWarehouseDir.toUri().resolve("warehouse").toString())
                    .set("spark.sql.warehouse.dir", localWarehouseDir.toUri().resolve("warehouse").toString());
            return this;
        }

        public Builder withS3(String s3Url) {
            sparkconf.set("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
                    .set("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
                    .set("spark.hadoop.fs.s3a.endpoint", s3Url)
                    .set("spark.hadoop.fs.s3a.endpoint.region", S3_REGION_NAME)
                    .set("spark.hadoop.fs.s3a.path.style.access", "true")
                    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                    .set("spark.sql.catalog.spark_catalog.type", "hadoop")
                    .set("spark.sql.catalog.spark_catalog.warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse")
                    .set("spark.sql.warehouse.dir", "s3a://" + S3_BUCKET + "/iceberg_warehouse");
            return this;
        }

        public SparkTestHelper build() {
            return new SparkTestHelper(SparkSession
                    .builder()
                    .config(sparkconf)
                    .getOrCreate());
        }
    }
}
