package db.engine.exec;

import java.util.Arrays;
import java.util.List;

/**
 * CompoundPredicate composes child predicates with logical AND / OR / NOT.
 * Supports variable arity for AND / OR and a single child for NOT.
 * Short-circuits evaluation for efficiency.
 */
public final class CompoundPredicate implements Predicate {
    public enum Type { AND, OR, NOT }

    private final Type type;
    private final List<Predicate> children; // for NOT size == 1

    private CompoundPredicate(Type type, List<Predicate> children) {
        if (type == Type.NOT && children.size() != 1) {
            throw new IllegalArgumentException("NOT requires exactly one child predicate");
        }
        if ((type == Type.AND || type == Type.OR) && children.size() < 2) {
            throw new IllegalArgumentException(type + " requires at least two child predicates");
        }
        this.type = type;
        this.children = children;
    }

    public static CompoundPredicate and(Predicate... predicates) {
        return new CompoundPredicate(Type.AND, Arrays.asList(predicates));
    }

    public static CompoundPredicate or(Predicate... predicates) {
        return new CompoundPredicate(Type.OR, Arrays.asList(predicates));
    }

    public static CompoundPredicate not(Predicate predicate) {
        return new CompoundPredicate(Type.NOT, List.of(predicate));
    }

    @Override
    public boolean test(Row row) {
        return switch (type) {
            case AND -> {
                for (Predicate p : children) if (!p.test(row)) { yield false; }
                yield true;
            }
            case OR -> {
                for (Predicate p : children) if (p.test(row)) { yield true; }
                yield false;
            }
            case NOT -> !children.get(0).test(row);
        };
    }

    // For debugging
    @Override
    public String toString() {
        return switch (type) {
            case AND -> join("AND");
            case OR -> join("OR");
            case NOT -> "NOT(" + children.get(0) + ")";
        };
    }

    private String join(String op) {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < children.size(); i++) {
            if (i > 0) sb.append(' ').append(op).append(' ');
            sb.append(children.get(i));
        }
        sb.append(')');
        return sb.toString();
    }
}
