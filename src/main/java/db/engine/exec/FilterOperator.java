package db.engine.exec;

/**
 * Operator that filters rows from its child based on a Predicate.
 * Pulls rows until one matches or child is exhausted.
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
    public Row next() {
        Row r;
        while ((r = child.next()) != null) {
            if (predicate.test(r)) return r;
        }
        return null;
    }

    @Override
    public void close() { child.close(); }
}
