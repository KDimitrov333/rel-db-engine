package db.engine.exec;

import java.util.List;
import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;

/**
 * Equality predicate for BOOLEAN or VARCHAR
 * For non-INT types only equality is supported; for INT you can still use ComparisonPredicate.
 */
public class EqualityPredicate implements Predicate {
    private final int columnIndex;
    private final Object expected;

    public EqualityPredicate(int columnIndex, Object expected) {
        this.columnIndex = columnIndex;
        this.expected = expected;
    }

    public static EqualityPredicate forColumnName(List<ColumnSchema> schema, String columnName, Object expected) {
        for (int i = 0; i < schema.size(); i++) {
            ColumnSchema c = schema.get(i);
            if (c.name().equals(columnName)) {
                // Type sanity check: allow INT/BOOLEAN/VARCHAR but require runtime type match
                DataType dt = c.type();
                switch (dt) {
                    case INT -> { if (!(expected instanceof Integer)) throw typeMismatch(columnName, dt, expected); }
                    case BOOLEAN -> { if (!(expected instanceof Boolean)) throw typeMismatch(columnName, dt, expected); }
                    case VARCHAR -> { if (!(expected instanceof String)) throw typeMismatch(columnName, dt, expected); }
                }
                return new EqualityPredicate(i, expected);
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    private static IllegalArgumentException typeMismatch(String col, DataType dt, Object v) {
        return new IllegalArgumentException("Expected value of type " + dt + " for column '" + col + "' but got " + (v == null ? "null" : v.getClass().getSimpleName()));
    }

    @Override
    public boolean test(Row row) {
        Object v = row.values().get(columnIndex);
        return expected == null ? v == null : expected.equals(v);
    }

    // For debugging
    @Override
    public String toString() { return "col[" + columnIndex + "] = " + expected; }
}
