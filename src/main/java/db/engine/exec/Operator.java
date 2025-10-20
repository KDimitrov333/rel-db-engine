package db.engine.exec;

import java.util.List;

import db.engine.catalog.ColumnSchema;

/**
 * Minimal physical operator interface
 */
public interface Operator {
    void open();
    Row next(); // returns next row or null when exhausted
    void close();

    /**
     * Optional schema metadata for produced rows. Operators that can supply it should override.
     * Returning null means schema unknown/not propagated.
     */
    default List<ColumnSchema> schema() { return null; }
}
