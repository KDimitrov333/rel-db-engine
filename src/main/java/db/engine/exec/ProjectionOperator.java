package db.engine.exec;

import java.util.ArrayList;
import java.util.List;

import db.engine.storage.Record;
import db.engine.catalog.ColumnSchema;

/**
 * Projection operator: selects a subset of columns from child rows.
 * Keeps the original RID so updates/deletes remain possible downstream.
 * Column selection is by index or by resolved column name into the child's values list.
 */
public class ProjectionOperator implements Operator {
    private final Operator child;
    private final int[] columnIndexes; // indices to keep in output order
    private List<ColumnSchema> cachedSchema; // built on first row or from child schema

    public ProjectionOperator(Operator child, int[] columnIndexes) {
        this.child = child;
        this.columnIndexes = columnIndexes;
    }

    /**
     * Build a ProjectionOperator by resolving column names against child schema.
     */
    public static ProjectionOperator forColumnNames(Operator child, List<String> columnNames) {
        if (columnNames == null || columnNames.isEmpty()) throw new IllegalArgumentException("columnNames must be non-empty");
        List<ColumnSchema> childSchema = child.schema();
        if (childSchema == null) throw new IllegalStateException("Child schema required for name-based projection");
        int[] idxs = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            String name = columnNames.get(i);
            int found = -1;
            for (int j = 0; j < childSchema.size(); j++) {
                if (childSchema.get(j).name().equals(name)) { found = j; break; }
            }
            if (found == -1) throw new IllegalArgumentException("Column not found in child schema: " + name);
            idxs[i] = found;
        }
        return new ProjectionOperator(child, idxs);
    }

    @Override
    public void open() { child.open(); }

    @Override
    public Row next() {
        Row r = child.next();
        if (r == null) return null;
        List<Object> src = r.values();
        List<Object> projected = new ArrayList<>(columnIndexes.length);
        for (int idx : columnIndexes) {
            projected.add(src.get(idx));
        }
        Record newRec = new Record(projected);
        // Build projected schema if original row had schema metadata
        List<ColumnSchema> projectedSchema = null;
        List<ColumnSchema> original = r.schema();
        if (original != null) {
            projectedSchema = new ArrayList<>(columnIndexes.length);
            for (int idx : columnIndexes) projectedSchema.add(original.get(idx));
        }
        cachedSchema = projectedSchema; // cache first built schema
        return Row.of(newRec, r.rid(), projectedSchema);
    }

    @Override
    public void close() { child.close(); }
    
    @Override
    public java.util.List<ColumnSchema> schema() { return cachedSchema != null ? cachedSchema : child.schema(); }
}
