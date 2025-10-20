package db.engine.query;

import java.util.List;

import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.exec.Predicate;
import db.engine.exec.ComparisonPredicate;
import db.engine.exec.EqualityPredicate;

/**
 * Compiles a logical Condition inside a SelectQuery into a physical Predicate.
 * Returns null if the query has no condition.
 */
public class PredicateCompiler {

    public Predicate compile(SelectQuery query, List<ColumnSchema> schema) {
        if (query == null) throw new IllegalArgumentException("query must not be null");
        Condition cond = query.condition();
        if (cond == null) return null;
        // Locate column and type
        int colIndex = -1; ColumnSchema colSchema = null;
        for (int i = 0; i < schema.size(); i++) {
            ColumnSchema c = schema.get(i);
            if (c.name().equals(cond.columnName())) { colIndex = i; colSchema = c; break; }
        }
        if (colIndex == -1) throw new IllegalArgumentException("Predicate column not found: " + cond.columnName());
        Object lit = cond.literalValue();
        DataType type = colSchema.type();
        switch (type) {
            case INT -> {
                if (!(lit instanceof Integer iv)) {
                    throw new IllegalArgumentException("Expected INT literal for column '" + colSchema.name() + "' but got " + (lit == null ? "null" : lit.getClass().getSimpleName()));
                }
                ComparisonPredicate.Op mapped = mapOp(cond.op());
                return new ComparisonPredicate(colIndex, mapped, iv);
            }
            case BOOLEAN, VARCHAR -> {
                if (cond.op() != Condition.Op.EQ) {
                    throw new IllegalArgumentException("Only EQ supported for non-INT column '" + colSchema.name() + "'");
                }
                return EqualityPredicate.forColumnName(schema, colSchema.name(), lit);
            }
            default -> throw new IllegalStateException("Unsupported type: " + type);
        }
    }

    private ComparisonPredicate.Op mapOp(Condition.Op op) {
        return switch (op) {
            case EQ -> ComparisonPredicate.Op.EQ;
            case LT -> ComparisonPredicate.Op.LT;
            case LTE -> ComparisonPredicate.Op.LTE;
            case GT -> ComparisonPredicate.Op.GT;
            case GTE -> ComparisonPredicate.Op.GTE;
        };
    }
}
