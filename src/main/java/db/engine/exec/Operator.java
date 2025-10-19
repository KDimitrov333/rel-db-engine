package db.engine.exec;

/**
 * Minimal physical operator interface
 */
public interface Operator {
    void open();
    Tuple next(); // returns next tuple or null when exhausted
    void close();
}
