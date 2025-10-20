package db.engine.query;

/**
 * Single-column WHERE condition descriptor.
 * Only comparison against a literal for now. Logical combinations come later.
 */
public class Condition {
    public enum Op { EQ, LT, LTE, GT, GTE }
    private final String columnName;
    private final Op op;
    private final Object literalValue; // expected to match column type

    public Condition(String columnName, Op op, Object literalValue) {
        this.columnName = columnName;
        this.op = op;
        this.literalValue = literalValue;
    }

    public String columnName() { return columnName; }
    public Op op() { return op; }
    public Object literalValue() { return literalValue; }
}
