package db.engine.exec;

import java.util.List;

import db.engine.catalog.ColumnSchema;

/**
 * Basic integer comparison predicate on a single column.
 * Supports operators: EQ, LT, LTE, GT, GTE.
 * Restricts to INT columns
 */
public class ComparisonPredicate implements Predicate {
    public enum Op { EQ, LT, LTE, GT, GTE }

    private final int columnIndex;
    private final Op op;
    private final int value;

    public ComparisonPredicate(int columnIndex, Op op, int value) {
        this.columnIndex = columnIndex;
        this.op = op;
        this.value = value;
    }

    public static ComparisonPredicate forColumnName(List<ColumnSchema> schema, String columnName, Op op, int value) {
        for (int i = 0; i < schema.size(); i++) {
            ColumnSchema c = schema.get(i);
            if (c.name().equals(columnName)) {
                if (c.type() != db.engine.catalog.DataType.INT) {
                    throw new IllegalArgumentException("ComparisonPredicate supports INT columns only: " + columnName);
                }
                return new ComparisonPredicate(i, op, value);
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    @Override
    public boolean test(Tuple tuple) {
        int v = (Integer) tuple.values().get(columnIndex);
        return switch (op) {
            case EQ -> v == value;
            case LT -> v < value;
            case LTE -> v <= value;
            case GT -> v > value;
            case GTE -> v >= value;
        };
    }
}
