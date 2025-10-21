package db.engine.query;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL parser for SELECT form:
 *   SELECT <cols|*> FROM <table> [WHERE <column> <op> <literal>];
 * Supported operators: =, <, <=, >, >=
 * Literals: INT, BOOLEAN (true/false), single-quoted VARCHAR (no escaping).
 * Single table only for now.
 */
public class QueryParser {
    private static final Pattern SELECT_PATTERN = Pattern.compile(
        // Pattern breakdown:
        //  SELECT\s+(.+)           -> group(1): column list or '*'
        //  FROM\s+([a-zA-Z_][a-zA-Z0-9_]*) -> group(2): table name (simple identifier)
        //  (?:\s+WHERE\s+([a-zA-Z_][a-zA-Z0-9_]*) -> group(3): WHERE column name
        //      \s*(=|<=|<|>=|>)               -> group(4): operator
        //      \s*([^;]+))?                   -> group(5): literal (up to semicolon or end)
        //  ;?$                                -> optional trailing semicolon
        // Entire WHERE clause is optional (non-capturing group with '?').
        "^SELECT\\s+(.+)\\s+FROM\\s+([a-zA-Z_][a-zA-Z0-9_]*)" +
        "(?:\\s+WHERE\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\s*(=|<=|<|>=|>)\\s*([^;]+))?;?$",
        Pattern.CASE_INSENSITIVE
    );

    public SelectQuery parse(String sql) {
        if (sql == null) throw new IllegalArgumentException("sql must not be null");
        String trimmed = sql.trim();
        Matcher m = SELECT_PATTERN.matcher(trimmed);
        if (!m.matches()) {
            throw new IllegalArgumentException("Malformed SELECT: " + sql);
        }
        String colsPart = m.group(1).trim();
        String tableName = m.group(2).trim();
        String whereCol = m.group(3);
        String whereOp = m.group(4);
        String whereLit = m.group(5);

        List<String> columns = new ArrayList<>();
        if (!colsPart.equals("*")) {
            for (String c : colsPart.split(",")) {
                String id = c.trim();
                if (id.isEmpty()) throw new IllegalArgumentException("Empty column name");
                columns.add(id);
            }
        }

        Condition condition = null;
        if (whereCol != null) {
            Condition.Op op = mapOp(whereOp);
            Object lit = parseLiteral(whereLit.trim());
            condition = new Condition(whereCol.trim(), op, lit);
        }
        return new SelectQuery(tableName, columns, condition);
    }

    private Condition.Op mapOp(String op) {
        return switch (op) {
            case "=" -> Condition.Op.EQ;
            case "<" -> Condition.Op.LT;
            case "<=" -> Condition.Op.LTE;
            case ">" -> Condition.Op.GT;
            case ">=" -> Condition.Op.GTE;
            default -> throw new IllegalArgumentException("Unsupported operator: " + op);
        };
    }

    private Object parseLiteral(String raw) {
        if (raw.startsWith("'") && raw.endsWith("'") && raw.length() >= 2) {
            return raw.substring(1, raw.length() - 1);
        }
        String lower = raw.toLowerCase();
        if (lower.equals("true")) return Boolean.TRUE;
        if (lower.equals("false")) return Boolean.FALSE;
        if (raw.matches("-?\\d+")) {
            try { return Integer.parseInt(raw); } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid integer literal: " + raw, e);
            }
        }
        throw new IllegalArgumentException("Unsupported literal: " + raw);
    }
}
