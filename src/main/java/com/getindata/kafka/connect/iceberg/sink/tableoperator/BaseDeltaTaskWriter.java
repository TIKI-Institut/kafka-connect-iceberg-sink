package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import org.apache.iceberg.*;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.TypeUtil;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import static com.getindata.kafka.connect.iceberg.sink.tableoperator.CdcOperation.DELETE;

public abstract class BaseDeltaTaskWriter extends BaseTaskWriter<Record> {

    private final Schema schema;
    private final Schema deleteSchema;

    private final InternalRecordWrapper wrapper;

    private final InternalRecordWrapper keyWrapper;

    private final boolean upsert;
    private final boolean upsertKeepDeletes;
    private final String cdcOpField;

    public BaseDeltaTaskWriter(PartitionSpec spec,
                               FileFormat format,
                               FileAppenderFactory<Record> appenderFactory,
                               OutputFileFactory fileFactory,
                               FileIO io,
                               long targetFileSize,
                               Schema schema,
                               Set<Integer> equalityFieldIds,
                               boolean upsert,
                               boolean upsertKeepDeletes,
                               String cdcOpField) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.schema = schema;
        this.deleteSchema = TypeUtil.select(schema, equalityFieldIds);
        this.wrapper = new InternalRecordWrapper(schema.asStruct());
        this.keyWrapper = new InternalRecordWrapper(deleteSchema.asStruct());
        this.upsert = upsert;
        this.upsertKeepDeletes = upsertKeepDeletes;
        this.cdcOpField = cdcOpField;
    }

    abstract RecordDeltaWriter route(Record row);

    InternalRecordWrapper wrapper() {
        return wrapper;
    }

    @Override
    public void write(Record row) throws IOException {
        RecordDeltaWriter writer = route(row);

        Optional<CdcOperation> cdcOp = Optional.ofNullable(row.getField(this.cdcOpField))
                .map(Object::toString)
                .map(CdcOperation::getByCode);

        boolean isDelete = cdcOp.equals(Optional.of(DELETE));

        if (upsert || isDelete) {
            // only use deleteKey due to:
            // - https://github.com/apache/iceberg/pull/4364
            // - and deduplication feature (possible that old record isn't present in the table)
            writer.deleteKey(row);
        }

        // if its deleted row and upsertKeepDeletes = true then add deleted record to target table
        // else deleted records are deleted from target table
        if (upsertKeepDeletes || !isDelete) {
            writer.write(row);
        }
    }

    public class RecordDeltaWriter extends BaseEqualityDeltaWriter {
        RecordDeltaWriter(PartitionKey partition) {
            super(partition, schema, deleteSchema);
        }

        @Override
        protected StructLike asStructLike(Record data) {
            return wrapper.wrap(data);
        }

        @Override
        protected StructLike asStructLikeKey(Record data) {
            return keyWrapper.wrap(data);
        }
    }
}
