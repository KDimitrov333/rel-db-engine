package db.engine.exec;

/**
 * Minimal physical operator interface
 */
public interface Operator {
    void open();
    Row next(); // returns next row or null when exhausted
    void close();
}
