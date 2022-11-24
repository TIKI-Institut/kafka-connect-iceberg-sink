package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import com.getindata.kafka.connect.iceberg.sink.IcebergUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

public class PartitionedDeltaWriter extends BaseDeltaTaskWriter {

    private final PartitionKey partitionKey;

    private final Map<PartitionKey, RecordDeltaWriter> writers = Maps.newHashMap();

    public PartitionedDeltaWriter(PartitionSpec spec,
                                  FileFormat format,
                                  FileAppenderFactory<Record> appenderFactory,
                                  OutputFileFactory fileFactory,
                                  FileIO io,
                                  long targetFileSize,
                                  Schema schema,
                                  boolean upsert,
                                  boolean upsertKeepDeletes,
                                  String cdcOpField) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema,
                IcebergUtil.getEqualityFieldIds(spec, schema, upsert), upsert, upsertKeepDeletes, cdcOpField);
        this.partitionKey = new PartitionKey(spec, schema);
    }

    @Override
    RecordDeltaWriter route(Record row) {
        partitionKey.partition(wrapper().wrap(row));

        return writers.computeIfAbsent(partitionKey, (key) -> {
            // NOTICE: we need to copy a new partition key here, in case of messing up the keys in writers.
            PartitionKey copiedKey = key.copy();
            return new RecordDeltaWriter(copiedKey);
        });
    }

    @Override
    public void close() {
        try {
            Tasks.foreach(writers.values())
                    .throwFailureWhenFinished()
                    .noRetry()
                    .run(RecordDeltaWriter::close, IOException.class);

            writers.clear();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close equality delta writer", e);
        }
    }
}
