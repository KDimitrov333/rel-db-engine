package db.engine.exec;

import java.util.ArrayList;
import java.util.List;

import db.engine.storage.Record;
import db.engine.catalog.ColumnSchema;

/**
 * Projection operator: selects a subset of columns from child tuples.
 * Keeps the original RID so updates/deletes remain possible downstream.
 * Column selection is by index into the child's values list.
 */
public class ProjectionOperator implements Operator {
    private final Operator child;
    private final int[] columnIndexes; // indices to keep in output order

    public ProjectionOperator(Operator child, int[] columnIndexes) {
        this.child = child;
        this.columnIndexes = columnIndexes;
    }

    @Override
    public void open() { child.open(); }

    @Override
    public Row next() {
        Row t = child.next();
        if (t == null) return null;
        List<Object> src = t.values();
        List<Object> projected = new ArrayList<>(columnIndexes.length);
        for (int idx : columnIndexes) {
            projected.add(src.get(idx));
        }
        Record newRec = new Record(projected);
        // Build projected schema if original tuple had schema metadata
        List<ColumnSchema> projectedSchema = null;
        List<ColumnSchema> original = t.schema();
        if (original != null) {
            projectedSchema = new ArrayList<>(columnIndexes.length);
            for (int idx : columnIndexes) projectedSchema.add(original.get(idx));
        }
        return Row.of(newRec, t.rid(), projectedSchema);
    }

    @Override
    public void close() { child.close(); }
}
