package db.engine.query;

import java.util.List;

/**
 * Logical SELECT query representation.
 * columns: empty list means SELECT *.
 * condition: optional single-column predicate; null => no WHERE.
 */
public record SelectQuery(String tableName, List<String> columns, Condition condition) {}
