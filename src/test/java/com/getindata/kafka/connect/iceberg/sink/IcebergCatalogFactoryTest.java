package com.getindata.kafka.connect.iceberg.sink;

import com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testing iceberg catalog creation to ensure classpath is set up correctly
 */
public class IcebergCatalogFactoryTest {

    @Test
    public void createHadoopCatalog(@TempDir Path localWarehouseDir) {
        IcebergSinkConfiguration config = TestConfig.builder()
                .withLocalCatalog(localWarehouseDir)
                .build();

        var catalog = IcebergCatalogFactory.create(config);

        assertThat(catalog).isInstanceOf(HadoopCatalog.class);
    }

    @Test
    public void createHiveCatalog() {
        IcebergSinkConfiguration config = TestConfig.builder()
                .withCustomCatalogProperty("type", "hive")
                .build();

        var catalog = IcebergCatalogFactory.create(config);

        assertThat(catalog).isInstanceOf(HiveCatalog.class);
    }


    @Test
    public void createNessieCatalog() {
        IcebergSinkConfiguration config = TestConfig.builder()
                .withCustomCatalogProperty("catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
                .withCustomCatalogProperty("uri", "http://dummy")
                .withCustomCatalogProperty("warehouse", "file://warehouse")
                .build();

        var catalog = IcebergCatalogFactory.create(config);

        assertThat(catalog).isInstanceOf(NessieCatalog.class);
    }

    @Test
    public void createGlueCatalog() {
        new GlueCatalog();
    }
}
