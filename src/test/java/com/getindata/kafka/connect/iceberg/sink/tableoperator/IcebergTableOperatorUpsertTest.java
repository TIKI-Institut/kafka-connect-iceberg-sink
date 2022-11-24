package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import com.clearspring.analytics.util.Lists;
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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergTableOperatorUpsertTest {

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
    public void testUpsert() {
        Schema schema = new Schema(
                List.of(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "data", Types.StringType.get())),
                Set.of(1)
        );

        var tableName = TableIdentifier.of("upsert", "simple");
        var sparkTableName = SparkTestHelper.getLocalCatalogTableName(tableName.toString());

        Table table1 = IcebergUtil.createIcebergTable(catalog, tableName, schema, Optional.empty());

        List<IcebergChangeEvent> events = new ArrayList<>();

        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 1)
                .addField("data", "val_old")
                .build()
        );
        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 2)
                .addField("data", "val")
                .build()
        );
        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 1)
                .addField("data", "val_new")
                .build()
        );

        icebergTableOperator.addToTable(table1, events);

        var tableContent = sparkTestHelper.table(sparkTableName).collectAsList();

        assertThat(tableContent.size()).isEqualTo(2);
        // row order of spark DF is not guaranteed, need to filter for element with ID 1
        assertThat(tableContent.stream().filter((r) -> r.getInt(0) == 1).collect(Collectors.toList())
                .get(0).getString(1)).isEqualTo("val_new");
    }

    @Test
    public void testUpsertOverBatches() {
        Schema schema = new Schema(
                List.of(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "data", Types.StringType.get())),
                Set.of(1)
        );

        var tableName = TableIdentifier.of("upsert", "overBatches");
        var sparkTableName = SparkTestHelper.getLocalCatalogTableName(tableName.toString());

        Table table1 = IcebergUtil.createIcebergTable(catalog, tableName, schema, Optional.empty());

        List<IcebergChangeEvent> events = new ArrayList<>();

        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 1)
                .addField("data", "val_old")
                .build()
        );
        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 2)
                .addField("data", "val")
                .build()
        );

        icebergTableOperator.addToTable(table1, events);

        var tableContent = sparkTestHelper.table(sparkTableName).collectAsList();

        assertThat(tableContent.size()).isEqualTo(2);
        // row order of spark DF is not guaranteed, need to filter for element with ID 1
        assertThat(tableContent.stream().filter((r) -> r.getInt(0) == 1).collect(Collectors.toList())
                .get(0).getString(1)).isEqualTo("val_old");

        // 2nd batch
        icebergTableOperator.addToTable(table1, List.of(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 1)
                .addField("data", "val_new")
                .build()
        ));

        var tableContentSecondBatch = sparkTestHelper.table(sparkTableName).collectAsList();

        assertThat(tableContentSecondBatch.size()).isEqualTo(2);
        // row order of spark DF is not guaranteed, need to filter for element with ID 1
        assertThat(tableContentSecondBatch.stream().filter((r) -> r.getInt(0) == 1).collect(Collectors.toList())
                .get(0).getString(1)).isEqualTo("val_new");

        // testing snapshots
        var snapshots = Lists.newArrayList(table1.snapshots());
        assertThat(snapshots.size()).isEqualTo(2);

        var oldSnapshotContent = sparkTestHelper.table(sparkTableName, snapshots.get(0).snapshotId()).collectAsList();
        assertThat(oldSnapshotContent.size()).isEqualTo(2);
        // row order of spark DF is not guaranteed, need to filter for element with ID 1
        assertThat(oldSnapshotContent.stream().filter((r) -> r.getInt(0) == 1).collect(Collectors.toList())
                .get(0).getString(1)).isEqualTo("val_old");
    }

    @Test
    public void testUpsertPartitioned() {
        Schema schema = new Schema(
                List.of(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "data", Types.StringType.get()),
                        Types.NestedField.required(3, "partition", Types.IntegerType.get())),
                Set.of(1)
        );

        var tableName = TableIdentifier.of("upsert", "partition");
        var sparkTableName = SparkTestHelper.getLocalCatalogTableName(tableName.toString());

        Table table1 = IcebergUtil.createIcebergTable(catalog, tableName, schema, Optional.empty());

        List<IcebergChangeEvent> events = new ArrayList<>();

        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 1)
                .addField("data", "val_old")
                .addField("partition", 1)
                .build()
        );
        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 2)
                .addField("data", "val")
                .addField("partition", 1)
                .build()
        );
        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 3)
                .addField("data", "val")
                .addField("partition", 2)
                .build()
        );
        events.add(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 1)
                .addField("data", "val_new")
                .addField("partition", 1)
                .build()
        );

        icebergTableOperator.addToTable(table1, events);

        var tableContent = sparkTestHelper.table(sparkTableName).collectAsList();

        assertThat(tableContent.size()).isEqualTo(3);
        // row order of spark DF is not guaranteed, need to filter for element with ID 1
        assertThat(tableContent.stream().filter((r) -> r.getInt(0) == 1).collect(Collectors.toList())
                .get(0).getString(1)).isEqualTo("val_new");

        // 2nd batch
        icebergTableOperator.addToTable(table1, List.of(new IcebergChangeEventBuilder()
                .destination(tableName.toString())
                .addKeyField("id", 1)
                .addField("data", "val_new2")
                .addField("partition", 2)
                .build()
        ));

        var tableContentSecondBatch = sparkTestHelper.table(sparkTableName).collectAsList();

        assertThat(tableContentSecondBatch.size()).isEqualTo(3);
        // row order of spark DF is not guaranteed, need to filter for element with ID 1
        assertThat(tableContentSecondBatch.stream().filter((r) -> r.getInt(0) == 1).collect(Collectors.toList())
                .get(0).getString(1)).isEqualTo("val_new2");
        assertThat(tableContentSecondBatch.stream().filter((r) -> r.getInt(0) == 1).collect(Collectors.toList())
                .get(0).getInt(2)).isEqualTo(2);
    }
}