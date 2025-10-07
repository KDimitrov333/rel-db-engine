package db.engine.catalog;

// Immutable data carrier for a table column.
// length: only matters for variable-length types like VARCHAR else may be 0.
public record ColumnSchema(String name, DataType type, int length) {}