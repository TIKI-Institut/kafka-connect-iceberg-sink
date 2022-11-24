package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;

import java.io.IOException;

public class UnpartitionedDeltaWriter extends BaseDeltaTaskWriter {
    private final RecordDeltaWriter writer;

    public UnpartitionedDeltaWriter(PartitionSpec spec,
                                    FileFormat format,
                                    FileAppenderFactory<Record> appenderFactory,
                                    OutputFileFactory fileFactory,
                                    FileIO io,
                                    long targetFileSize,
                                    Schema schema,
                                    boolean upsert,
                                    boolean upsertKeepDeletes,
                                    String cdcOpField) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema, schema.identifierFieldIds(), upsert, upsertKeepDeletes, cdcOpField);
        this.writer = new RecordDeltaWriter(null);
    }

    @Override
    RecordDeltaWriter route(Record row) {
        return writer;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
