package db.engine.exec;

import java.util.List;

import db.engine.storage.RID;
import db.engine.storage.Record;
import db.engine.catalog.ColumnSchema;

/**
 * Row is an execution pipeline unit (values + RID + optional schema metadata).
 * Record is storage-level serialization; Row wraps Record with identity and schema (optionally).
 */
public class Row {
    private final Record record;
    private final RID rid;
    private final List<ColumnSchema> schema; // can be null

    public static Row of(Record record, RID rid) { return new Row(record, rid, null); }
    public static Row of(Record record, RID rid, List<ColumnSchema> schema) { return new Row(record, rid, schema); }

    public Row(Record record, RID rid, List<ColumnSchema> schema) {
        this.record = record;
        this.rid = rid;
        this.schema = schema;
    }

    public Record record() { return record; }
    public RID rid() { return rid; }
    public List<Object> values() { return record.getValues(); }
    public List<ColumnSchema> schema() { return schema; }

    @Override
    public String toString() {
        return "Row" + values() + " rid=" + rid + (schema != null ? " schemaCols=" + schema.size() : "");
    }
}
