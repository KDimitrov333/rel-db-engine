package db.engine.catalog;

// Immutable data carrier for a table column.
public record ColumnSchema(String name, String type, int length) {}