package db.engine.exec;

/**
 * Operator that filters tuples from its child based on a Predicate.
 * Pulls tuples until one matches or child is exhausted.
 */
public class FilterOperator implements Operator {
    private final Operator child;
    private final Predicate predicate;

    public FilterOperator(Operator child, Predicate predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    @Override
    public void open() { child.open(); }

    @Override
    public Tuple next() {
        Tuple t;
        while ((t = child.next()) != null) {
            if (predicate.test(t)) return t;
        }
        return null;
    }

    @Override
    public void close() { child.close(); }
}
