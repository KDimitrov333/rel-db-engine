package db.engine.query;

import java.util.List;
import java.util.ArrayList;

import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.exec.Predicate;
import db.engine.exec.ComparisonPredicate;
import db.engine.exec.EqualityPredicate;
import db.engine.exec.CompoundPredicate;

/**
 * Compiles a logical Condition inside a SelectQuery into a physical Predicate.
 * Returns null if the query has no condition.
 */
public class PredicateCompiler {

    public Predicate compile(SelectQuery query, List<ColumnSchema> schema) {
        if (query == null) throw new IllegalArgumentException("query must not be null");
        WhereClause where = query.where();
        if (where == null) return null;
        // Build individual predicates then combine via connectors.
        Predicate[] atomic = new Predicate[where.conditions().size()];
        for (int i=0;i<where.conditions().size();i++) {
            atomic[i] = compileSingle(where.conditions().get(i), schema);
        }
        if (atomic.length == 1) return atomic[0];
        List<String> connectors = where.connectors();
        // Build sequentially honoring connectors order (no parentheses support). Group consecutive AND.
        List<Predicate> orGroups = new ArrayList<>();
        List<Predicate> currentAndGroup = new ArrayList<>();
        currentAndGroup.add(atomic[0]);
        for (int i=0;i<connectors.size();i++) {
            String conn = connectors.get(i);
            Predicate next = atomic[i+1];
            if (conn.equals("AND")) {
                currentAndGroup.add(next);
            } else { // OR
                // finalize current AND group
                Predicate andCombined = currentAndGroup.size()==1 ? currentAndGroup.get(0) : CompoundPredicate.and(currentAndGroup.toArray(new Predicate[0]));
                orGroups.add(andCombined);
                currentAndGroup = new ArrayList<>();
                currentAndGroup.add(next);
            }
        }
        // finalize last group
        Predicate lastAnd = currentAndGroup.size()==1 ? currentAndGroup.get(0) : CompoundPredicate.and(currentAndGroup.toArray(new Predicate[0]));
        orGroups.add(lastAnd);
        return orGroups.size()==1 ? orGroups.get(0) : CompoundPredicate.or(orGroups.toArray(new Predicate[0]));
    }

    private Predicate compileSingle(Condition cond, List<ColumnSchema> schema) {
        // Locate column and type
        int colIndex = -1; ColumnSchema colSchema = null;
        for (int i = 0; i < schema.size(); i++) {
            ColumnSchema c = schema.get(i);
            if (c.name().equals(cond.columnName())) { colIndex = i; colSchema = c; break; }
        }
        if (colIndex == -1) throw new IllegalArgumentException("Predicate column not found: " + cond.columnName());
        Object lit = cond.literalValue();
        DataType type = colSchema.type();
        Predicate base = switch (type) {
            case INT -> {
                if (!(lit instanceof Integer iv)) {
                    throw new IllegalArgumentException("Expected INT literal for column '" + colSchema.name() + "' but got " + (lit == null ? "null" : lit.getClass().getSimpleName()));
                }
                ComparisonPredicate.Op mapped = mapOp(cond.op());
                yield new ComparisonPredicate(colIndex, mapped, iv);
            }
            case BOOLEAN, VARCHAR -> {
                if (cond.op() != Condition.Op.EQ) {
                    throw new IllegalArgumentException("Only EQ supported for non-INT column '" + colSchema.name() + "'");
                }
                yield EqualityPredicate.forColumnName(schema, colSchema.name(), lit);
            }
            default -> throw new IllegalStateException("Unsupported type: " + type);
        };
        return cond.negated() ? CompoundPredicate.not(base) : base;
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
