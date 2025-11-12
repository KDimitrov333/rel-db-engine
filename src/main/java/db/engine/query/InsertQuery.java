package db.engine.query;

import java.util.List;

/** Logical representation of an INSERT statement. */
public record InsertQuery(String tableName, List<String> columns, List<Object> values) implements Query {
    public InsertQuery {
        if (tableName == null || tableName.isBlank()) throw new IllegalArgumentException("tableName required");
        if (columns == null || columns.isEmpty()) throw new IllegalArgumentException("columns required");
        if (values == null || values.isEmpty()) throw new IllegalArgumentException("values required");
        if (columns.size() != values.size()) throw new IllegalArgumentException("Column/value count mismatch: " + columns.size() + " vs " + values.size());
    }
}