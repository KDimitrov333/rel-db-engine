package db.engine.exec;

import db.engine.storage.Record;

/**
 * Minimal physical operator interface
 */
public interface Operator {
    void open();
    Record next(); // returns next record or null when exhausted
    void close();
}
