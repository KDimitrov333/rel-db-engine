package db.engine.query;

/**
 * Logical INNER JOIN specification: left table is SelectQuery.tableName, right table defined here.
 * Supports single equality predicate: leftColumn = rightTable.rightColumn.
 */
public record JoinSpec(String rightTable, String leftColumn, String rightColumn) {}
