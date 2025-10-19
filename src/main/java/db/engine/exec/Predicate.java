package db.engine.exec;

/**
 * Minimal predicate interface evaluated against a Tuple.
 */
public interface Predicate {
    boolean test(Tuple tuple);
}
