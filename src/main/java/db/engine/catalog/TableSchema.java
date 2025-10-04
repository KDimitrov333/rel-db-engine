package db.engine.catalog;

import java.util.List;

// Immutable data carrier for a table schema.
public record TableSchema(String name, List<ColumnSchema> columns, String filePath) {}