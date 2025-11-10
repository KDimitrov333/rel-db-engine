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
        // Pattern breakdown base portion (WHERE part captured as full tail for custom parsing):
        //  SELECT\s+(.+)            -> group(1): column list or '*'
        //  FROM\s+([identifier])    -> group(2): table name
        //  (?:\s+WHERE\s+(.+))?    -> group(3): full WHERE tail (we parse manually for AND/OR)
        //  ;?$                      -> optional semicolon
        "^SELECT\\s+(.+)\\s+FROM\\s+([a-zA-Z_][a-zA-Z0-9_]*)" +
        "(?:\\s+WHERE\\s+(.+))?;?$",
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
        String whereTail = m.group(3);

        List<String> columns = new ArrayList<>();
        if (!colsPart.equals("*")) {
            for (String c : colsPart.split(",")) {
                String id = c.trim();
                if (id.isEmpty()) throw new IllegalArgumentException("Empty column name");
                columns.add(id);
            }
        }

        WhereClause where = null;
        if (whereTail != null) {
            where = parseWhere(whereTail.trim());
        }
        return new SelectQuery(tableName, columns, where);
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

    // Parse simple AND/OR chain without parentheses.
    private WhereClause parseWhere(String raw) {
        String[] tokens = raw.split("\\s+");
        List<Condition> conditions = new ArrayList<>();
        List<String> connectors = new ArrayList<>();
        int i = 0;
        while (i < tokens.length) {
            boolean negated = false;
            if (tokens[i].equalsIgnoreCase("NOT")) {
                negated = true;
                i++;
                if (i >= tokens.length) throw new IllegalArgumentException("NOT without following comparison");
            }
            if (i + 2 >= tokens.length) throw new IllegalArgumentException("Incomplete comparison near token: " + tokens[i]);
            String col = tokens[i];
            String opTok = tokens[i+1];
            String litTok = tokens[i+2];
            Condition.Op op = mapOp(opTok);
            Object lit = parseLiteral(litTok);
            conditions.add(new Condition(col, op, lit, negated));
            i += 3;
            if (i < tokens.length) {
                String connector = tokens[i].toUpperCase();
                if (!connector.equals("AND") && !connector.equals("OR")) {
                    throw new IllegalArgumentException("Unexpected token (expected AND/OR): " + tokens[i]);
                }
                connectors.add(connector);
                i++;
            }
        }
        return new WhereClause(conditions, connectors);
    }
}
