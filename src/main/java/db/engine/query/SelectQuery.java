package db.engine.query;

import java.util.List;

/**
 * Logical SELECT query representation.
 * columns: empty list means SELECT *.
 * where: optional chain of conditions; null => no WHERE.
 */
public record SelectQuery(String tableName, List<String> columns, WhereClause where, JoinSpec join) implements Query {}
