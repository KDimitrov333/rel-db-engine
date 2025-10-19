package db.engine.exec;

import java.util.List;

import db.engine.storage.RID;
import db.engine.storage.Record;

/**
 * Lightweight wrapper for an output row flowing between operators.
 * Carries the deserialized values (Record) and the row identifier (RID)
 * so downstream operators can perform updates/deletes or build indexes.
 */
public class Tuple {
    private final Record record;
    private final RID rid;

    public Tuple(Record record, RID rid) {
        this.record = record;
        this.rid = rid;
    }

    public Record record() { return record; }
    public RID rid() { return rid; }
    public List<Object> values() { return record.getValues(); }

    @Override
    public String toString() { return "Tuple" + values() + " rid=" + rid; }
}
