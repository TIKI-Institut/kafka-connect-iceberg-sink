/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package com.getindata.kafka.connect.iceberg.sink;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Ismail Simsek
 */
public class IcebergUtil {
    protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);

    public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                           Schema schema, Optional<String> partitionField) {

        LOGGER.info("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schema,
                schema.identifierFieldNames());

        var partitionBuilder = PartitionSpec.builderFor(schema);

        final PartitionSpec ps = partitionField.flatMap((fieldName) -> {
            Optional<Types.NestedField> schemaField = Optional.ofNullable(schema.findField(fieldName));

            if (schemaField.isEmpty()) {
                LOGGER.warn("Table schema dont contain partition field {}! Creating table without partition", fieldName);
            }

            return schemaField;
        }).map((field) -> {
            switch (field.type().typeId()) {
                case DATE:
                case TIMESTAMP:
                    return partitionBuilder.day(field.name()).build();
                default:
                    return partitionBuilder.identity(field.name()).build();
            }
        }).orElseGet(partitionBuilder::build);

        return icebergCatalog.buildTable(tableIdentifier, schema)
                .withProperty(TableProperties.FORMAT_VERSION, "2")
                .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
                .withPartitionSpec(ps)
                .create();
    }

    private static SortOrder getIdentifierFieldsAsSortOrder(Schema schema) {
        SortOrder.Builder sob = SortOrder.builderFor(schema);
        for (String fieldName : schema.identifierFieldNames()) {
            sob = sob.asc(fieldName);
        }

        return sob.build();
    }

    public static Optional<Table> loadIcebergTable(Catalog icebergCatalog, TableIdentifier tableId) {
        try {
            Table table = icebergCatalog.loadTable(tableId);
            return Optional.of(table);
        } catch (NoSuchTableException e) {
            LOGGER.info("Table not found: {}", tableId.toString());
            return Optional.empty();
        }
    }

    public static FileFormat getTableFileFormat(Table icebergTable) {
        String formatAsString = icebergTable.properties().getOrDefault(
                TableProperties.DEFAULT_FILE_FORMAT,
                TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
    }

    public static long getTargetDataFileSize(Table table) {
        return Optional.ofNullable(table.properties().get(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES))
                .map(Long::parseLong)
                .orElse(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    }

    public static GenericAppenderFactory getTableAppender(Table icebergTable, boolean upsert) {
        return new GenericAppenderFactory(
                icebergTable.schema(),
                icebergTable.spec(),
                getEqualityFieldIds(icebergTable.spec(), icebergTable.schema(), upsert).stream().mapToInt(Number::intValue).toArray(),
                icebergTable.schema(),
                null);
    }

    public static Set<Integer> getEqualityFieldIds(PartitionSpec spec, Schema schema, boolean upsert) {
        var equalityFieldIds = Sets.newHashSet(schema.identifierFieldIds());
        if (upsert) {
            // Note: https://iceberg.apache.org/docs/latest/flink/#upsert
            // i am not sure why this is recommended, but just incase follow the docs ;)
            equalityFieldIds.addAll(spec.fields().stream().map(PartitionField::sourceId).collect(Collectors.toSet()));
        }
        return equalityFieldIds;
    }

}
