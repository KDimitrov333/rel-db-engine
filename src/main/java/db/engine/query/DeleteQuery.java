package db.engine.query;

/** Logical representation of DELETE statement. */
public record DeleteQuery(String tableName, WhereClause where) implements Query {
    public DeleteQuery {
        if (tableName == null || tableName.isBlank()) throw new IllegalArgumentException("tableName required");
    }
    public boolean hasWhere() { return where != null; }
}