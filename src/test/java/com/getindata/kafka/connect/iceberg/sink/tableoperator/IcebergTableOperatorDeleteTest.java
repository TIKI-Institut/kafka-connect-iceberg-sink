package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import com.getindata.kafka.connect.iceberg.sink.IcebergCatalogFactory;
import com.getindata.kafka.connect.iceberg.sink.IcebergChangeEvent;
import com.getindata.kafka.connect.iceberg.sink.IcebergSinkConfiguration;
import com.getindata.kafka.connect.iceberg.sink.IcebergUtil;
import com.getindata.kafka.connect.iceberg.sink.testresources.IcebergChangeEventBuilder;
import com.getindata.kafka.connect.iceberg.sink.testresources.SparkTestHelper;
import com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergTableOperatorDeleteTest {

    private static IcebergTableOperator icebergTableOperator;
    private static Catalog catalog;
    private static SparkTestHelper sparkTestHelper;

    @BeforeAll
    static void setup(@TempDir Path localWarehouseDir) {
        IcebergSinkConfiguration config = TestConfig.builder()
                .withLocalCatalog(localWarehouseDir)
                .withUpsert(true)
                .build();

        catalog = IcebergCatalogFactory.create(config);
        icebergTableOperator = IcebergTableOperatorFactory.create(config);
        sparkTestHelper = SparkTestHelper.builder().withLocalCatalog(localWarehouseDir).build();
    }

    @AfterAll
    static void cleanup() {
        sparkTestHelper.shutdownSession();
    }

    @Test
    public void testDelete() {
        Schema schema = new Schema(
                List.of(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "data", Types.StringType.get()),
                        Types.NestedField.required(3, "__op", Types.StringType.get())),
                Set.of(1)
        );

        var tableName = TableIdentifier.of("delete", "simple");
        var sparkTableName = SparkTestHelper.getLocalCatalogTableName(tableName.toString());

        Table table1 = IcebergUtil.createIcebergTable(catalog, tableName, schema, Optional.empty());

        List<IcebergChangeEvent> events = new ArrayList<>();

        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 1)
                .addField("data", "val")
                .addField("__op", "i")
                .build()
        );
        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 2)
                .addField("data", "val")
                .addField("__op", "i")
                .build()
        );
        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 1)
                .addField("data", "val")
                .addField("__op", "d")
                .build()
        );

        icebergTableOperator.addToTable(table1, events);

        var tableContent = sparkTestHelper.table(sparkTableName).collectAsList();

        assertThat(tableContent.size()).isEqualTo(1);
    }

    @Test
    public void testDeleteOverBatches() {
        Schema schema = new Schema(
                List.of(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "data", Types.StringType.get()),
                        Types.NestedField.required(3, "__op", Types.StringType.get())),
                Set.of(1)
        );

        var tableName = TableIdentifier.of("delete", "overBatches");
        var sparkTableName = SparkTestHelper.getLocalCatalogTableName(tableName.toString());

        Table table1 = IcebergUtil.createIcebergTable(catalog, tableName, schema, Optional.empty());

        List<IcebergChangeEvent> events = new ArrayList<>();

        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 1)
                .addField("data", "val")
                .addField("__op", "i")
                .build()
        );
        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 2)
                .addField("data", "val")
                .addField("__op", "i")
                .build()
        );

        icebergTableOperator.addToTable(table1, events);

        var tableContent = sparkTestHelper.table(sparkTableName).collectAsList();

        assertThat(tableContent.size()).isEqualTo(2);

        // 2nd batch
        icebergTableOperator.addToTable(table1, List.of(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 1)
                .addField("data", "val")
                .addField("__op", "d")
                .build()));

        var tableContentSecondBatch = sparkTestHelper.table(sparkTableName).collectAsList();

        assertThat(tableContentSecondBatch.size()).isEqualTo(1);
    }
}