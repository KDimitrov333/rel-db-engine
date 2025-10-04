package db.engine.catalog;

// Immutable data carrier for an index definition.
public record IndexSchema(String name, String table, String column, String filePath) {}