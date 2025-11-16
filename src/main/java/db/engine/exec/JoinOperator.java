package db.engine.exec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import db.engine.catalog.ColumnSchema;
import db.engine.storage.Record;

/**
 * Nested loop INNER JOIN operator for equality join: leftColumn == rightColumn.
 * Left and right children must both provide schema metadata.
 */
public class JoinOperator implements Operator {
    private final Operator left;
    private final Operator right;
    private final String leftColumn;
    private final String rightColumn;
    private List<ColumnSchema> joinedSchema;

    // Materialized right side keyed by join value for faster lookup
    private Map<Object, List<Row>> rightHash;
    private Row currentLeft;
    private Iterator<Row> currentMatches;

    public JoinOperator(Operator left, Operator right, String leftColumn, String rightColumn) {
        this.left = left; this.right = right; this.leftColumn = leftColumn; this.rightColumn = rightColumn;
    }

    @Override
    public void open() {
        left.open();
        right.open();
        List<ColumnSchema> leftSchema = left.schema();
        List<ColumnSchema> rightSchema = right.schema();
        if (leftSchema == null || rightSchema == null) {
            throw new IllegalStateException("JoinOperator requires both children to provide schema");
        }
        joinedSchema = new ArrayList<>(leftSchema.size() + rightSchema.size());
        joinedSchema.addAll(leftSchema);
        joinedSchema.addAll(rightSchema);
        rightHash = new HashMap<>();
        Row r;
        while ((r = right.next()) != null) {
            Object key = valueByName(r, rightColumn, rightSchema);
            rightHash.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
        }
        right.close(); // no longer needed
        currentLeft = left.next();
        currentMatches = Collections.emptyIterator();
    }

    @Override
    public Row next() {
        while (true) {
            if (currentLeft == null) return null;
            if (currentMatches.hasNext()) {
                Row matchRight = currentMatches.next();
                List<Object> combined = new ArrayList<>(currentLeft.values().size() + matchRight.values().size());
                combined.addAll(currentLeft.values());
                combined.addAll(matchRight.values());
                return Row.of(new Record(combined), currentLeft.rid(), joinedSchema);
            }
            // Advance left and prepare matches
            Object lVal = valueByName(currentLeft, leftColumn, left.schema());
            List<Row> matches = rightHash.get(lVal);
            if (matches != null) {
                currentMatches = matches.iterator();
            } else {
                currentMatches = Collections.emptyIterator();
            }
            // If no matches, advance left and loop again; if matches, loop will emit them.
            if (!currentMatches.hasNext()) {
                currentLeft = left.next();
                continue;
            }
        }
    }

    private Object valueByName(Row row, String columnName, List<ColumnSchema> schema) {
        for (int i=0;i<schema.size();i++) {
            if (schema.get(i).name().equals(columnName)) return row.values().get(i);
        }
        throw new IllegalArgumentException("Column not found in join schema: " + columnName);
    }

    @Override
    public void close() {
        left.close();
        // right already closed after build
        rightHash = null;
    }

    @Override
    public List<ColumnSchema> schema() { return joinedSchema; }
}
