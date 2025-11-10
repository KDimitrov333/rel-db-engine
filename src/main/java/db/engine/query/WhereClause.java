package db.engine.query;

import java.util.List;

/**
 * Represents a WHERE clause as a linear sequence of atomic Conditions combined by AND/OR.
 * Precedence: AND before OR
 * Example: col1 = 5 AND col2 > 3 OR col3 = 'x'
 * connectors list size == conditions.size()-1. Each connector is either "AND" or "OR".
 */
public class WhereClause {
    private final List<Condition> conditions;
    private final List<String> connectors; // AND/OR between conditions

    public WhereClause(List<Condition> conditions, List<String> connectors) {
        if (conditions == null || conditions.isEmpty()) throw new IllegalArgumentException("conditions empty");
        if (connectors.size() != conditions.size() - 1) throw new IllegalArgumentException("connectors mismatch");
        this.conditions = List.copyOf(conditions);
        this.connectors = List.copyOf(connectors);
    }

    public List<Condition> conditions() { return conditions; }
    public List<String> connectors() { return connectors; }
    public boolean isSingle() { return conditions.size() == 1; }
}