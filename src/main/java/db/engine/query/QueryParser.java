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
        // Extended pattern adds optional JOIN ... ON ... before WHERE.
        // Groups:
        // 1: column list or '*'
        // 2: base (left) table name
        // 3: optional right table in JOIN
        // 4: optional left qualified column (from ON clause left side)
        // 5: optional right qualified column (from ON clause right side)
        // 6: optional WHERE tail
        "^SELECT\\s+(.+)\\s+FROM\\s+([a-zA-Z_][a-zA-Z0-9_]*)" +
        "(?:\\s+JOIN\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\s+ON\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\s*=\\s*([a-zA-Z_][a-zA-Z0-9_]*))?" +
        "(?:\\s+WHERE\\s+(.+))?;?$",
        Pattern.CASE_INSENSITIVE
    );

    public SelectQuery parseSelect(String sql) {
        if (sql == null) throw new IllegalArgumentException("sql must not be null");
        String trimmed = sql.trim();
        Matcher m = SELECT_PATTERN.matcher(trimmed);
        if (!m.matches()) {
            throw new IllegalArgumentException("Malformed SELECT: " + sql);
        }
        String colsPart = m.group(1).trim();
        String tableName = m.group(2).trim();
        String joinRight = m.group(3) != null ? m.group(3).trim() : null;
        String joinLeftCol = m.group(4) != null ? m.group(4).trim() : null;
        String joinRightCol = m.group(5) != null ? m.group(5).trim() : null;
        String whereTail = m.group(6);

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
        JoinSpec join = null;
        if (joinRight != null) {
            if (joinLeftCol == null || joinRightCol == null) {
                throw new IllegalArgumentException("JOIN ON clause requires two column identifiers");
            }
            join = new JoinSpec(joinRight, joinLeftCol, joinRightCol);
        }
        return new SelectQuery(tableName, columns, where, join);
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

    // Parse simple AND/OR chain without parentheses. Respects single quotes in literals.
    private WhereClause parseWhere(String raw) {
        List<String> tokens = tokenizeWhere(raw);
        List<Condition> conditions = new ArrayList<>();
        List<String> connectors = new ArrayList<>();
        int i = 0;
        while (i < tokens.size()) {
            boolean negated = false;
            if (tokens.get(i).equalsIgnoreCase("NOT")) {
                negated = true;
                i++;
                if (i >= tokens.size()) throw new IllegalArgumentException("NOT without following comparison");
            }
            if (i >= tokens.size()) throw new IllegalArgumentException("Unexpected end after NOT");
            String col = tokens.get(i);
            // Decide if this is a bare boolean column (next token is AND/OR or end) or a full comparison
            if (i + 1 >= tokens.size()) {
                // Bare boolean at end: treat as col = TRUE
                conditions.add(new Condition(col, Condition.Op.EQ, Boolean.TRUE, negated));
                i += 1;
            } else {
                String next = tokens.get(i+1);
                String upperNext = next.toUpperCase();
                boolean isConnector = upperNext.equals("AND") || upperNext.equals("OR");
                boolean isOp = next.equals("=") || next.equals("<") || next.equals("<=") || next.equals(">") || next.equals(">=");
                if (isConnector) {
                    // Bare boolean before connector
                    conditions.add(new Condition(col, Condition.Op.EQ, Boolean.TRUE, negated));
                    i += 1; // consume column only
                } else if (isOp) {
                    // Full comparison requires literal token
                    if (i + 2 >= tokens.size()) throw new IllegalArgumentException("Incomplete comparison near token: " + tokens.get(i));
                    String opTok = next;
                    String litTok = tokens.get(i+2);
                    Condition.Op op = mapOp(opTok);
                    Object lit = parseLiteral(litTok);
                    conditions.add(new Condition(col, op, lit, negated));
                    i += 3; // consumed column op literal
                } else {
                    throw new IllegalArgumentException("Unexpected token (expected operator or AND/OR): " + next);
                }
            }
            if (i < tokens.size()) {
                String connector = tokens.get(i).toUpperCase();
                if (!connector.equals("AND") && !connector.equals("OR")) {
                    throw new IllegalArgumentException("Unexpected token (expected AND/OR): " + tokens.get(i));
                }
                connectors.add(connector);
                i++;
            }
        }
        return new WhereClause(conditions, connectors);
    }

    // Tokenize WHERE clause respecting single-quoted strings (no escaping inside quotes).
    private List<String> tokenizeWhere(String raw) {
        List<String> out = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < raw.length(); i++) {
            char ch = raw.charAt(i);
            if (ch == '\'') {
                inQuote = !inQuote;
                current.append(ch);
            } else if (Character.isWhitespace(ch) && !inQuote) {
                if (current.length() > 0) {
                    out.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(ch);
            }
        }
        if (current.length() > 0) out.add(current.toString());
        return out;
    }

    // INSERT INTO tableName (col1, col2, ...) VALUES (val1, val2, ...);
    private static final Pattern INSERT_PATTERN = Pattern.compile(
        "^INSERT\\s+INTO\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\(([^)]+)\\)\\s+VALUES\\s*\\(([^)]+)\\)\\s*;?$",
        Pattern.CASE_INSENSITIVE);

    // DELETE FROM tableName [WHERE <pred>];
    private static final Pattern DELETE_BASE_PATTERN = Pattern.compile(
        "^DELETE\\s+FROM\\s+([a-zA-Z_][a-zA-Z0-9_]*)" +
        "(?:\\s+WHERE\\s+(.+))?;?$",
        Pattern.CASE_INSENSITIVE);

    public InsertQuery parseInsert(String sql) {
        if (sql == null) throw new IllegalArgumentException("sql must not be null");
        String trimmed = sql.trim();
        Matcher m = INSERT_PATTERN.matcher(trimmed);
        if (!m.matches()) throw new IllegalArgumentException("Malformed INSERT: " + sql);
        String table = m.group(1).trim();
        String colsPart = m.group(2).trim();
        String valsPart = m.group(3).trim();
        List<String> cols = new ArrayList<>();
        for (String c : colsPart.split(",")) {
            String col = c.trim(); if (col.isEmpty()) throw new IllegalArgumentException("Empty column name in INSERT");
            cols.add(col);
        }
        List<Object> vals = new ArrayList<>();
        for (String vRaw : splitValues(valsPart)) {
            vals.add(parseLiteral(vRaw.trim()));
        }
        if (cols.size() != vals.size()) throw new IllegalArgumentException("Columns count " + cols.size() + " != values count " + vals.size());
        return new InsertQuery(table, cols, vals);
    }

    // Split values respecting commas inside single quotes
    private java.util.List<String> splitValues(String raw) {
        List<String> out = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        for (int i=0;i<raw.length();i++) {
            char ch = raw.charAt(i);
            if (ch == '\'') {
                inQuote = !inQuote;
                current.append(ch);
            } else if (ch == ',' && !inQuote) {
                out.add(current.toString()); current.setLength(0);
            } else {
                current.append(ch);
            }
        }
        if (current.length() > 0) out.add(current.toString());
        return out;
    }

    public DeleteQuery parseDelete(String sql) {
        if (sql == null) throw new IllegalArgumentException("sql must not be null");
        String trimmed = sql.trim();
        Matcher m = DELETE_BASE_PATTERN.matcher(trimmed);
        if (!m.matches()) throw new IllegalArgumentException("Malformed DELETE: " + sql);
        String table = m.group(1).trim();
        String whereTail = m.group(2);
        WhereClause where = null;
        if (whereTail != null) where = parseWhere(whereTail.trim());
        return new DeleteQuery(table, where);
    }
}
