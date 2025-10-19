package db.engine.exec;

/**
 * Minimal predicate interface evaluated against a Row.
 */
public interface Predicate {
    boolean test(Row row);
}
