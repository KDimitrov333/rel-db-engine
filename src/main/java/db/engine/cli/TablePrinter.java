package db.engine.cli;

import java.util.List;

import db.engine.catalog.ColumnSchema;
import db.engine.exec.Row;

/**
 * Simple ASCII table printer for query result rows.
 * Uses schema metadata when available to render headers.
 */
public final class TablePrinter {
    private TablePrinter() {}

    public static void print(List<Row> rows) {
        if (rows == null || rows.isEmpty()) {
            System.out.println("(0 row(s))");
            return;
        }
        Row first = rows.get(0);
        List<ColumnSchema> schema = first.schema();
        int colCount = first.values().size();
        String[] headers = new String[colCount];
        for (int i = 0; i < colCount; i++) {
            headers[i] = schema != null ? schema.get(i).name() : ("col" + (i + 1));
        }
        int[] widths = new int[colCount];
        for (int i = 0; i < colCount; i++) widths[i] = headers[i].length();
        for (Row r : rows) {
            List<Object> vals = r.values();
            for (int i = 0; i < colCount; i++) {
                String s = String.valueOf(vals.get(i));
                if (s.length() > widths[i]) widths[i] = s.length();
            }
        }
        String divLine = buildDivider(widths);
        System.out.println(divLine);
        System.out.println(buildHeader(headers, widths));
        System.out.println(divLine);
        for (Row r : rows) {
            System.out.println(buildRow(r, widths));
        }
        System.out.println(divLine);
        System.out.println("(" + rows.size() + " row(s))");
    }

    private static String buildDivider(int[] widths) {
        StringBuilder divider = new StringBuilder();
        divider.append('+');
        for (int w : widths) {
            for (int k = 0; k < w + 2; k++) divider.append('-');
            divider.append('+');
        }
        return divider.toString();
    }

    private static String buildHeader(String[] headers, int[] widths) {
        StringBuilder sb = new StringBuilder();
        sb.append('|');
        for (int i = 0; i < headers.length; i++) {
            sb.append(' ').append(pad(headers[i], widths[i])).append(' ').append('|');
        }
        return sb.toString();
    }

    private static String buildRow(Row r, int[] widths) {
        StringBuilder sb = new StringBuilder();
        sb.append('|');
        List<Object> vals = r.values();
        for (int i = 0; i < widths.length; i++) {
            String s = String.valueOf(vals.get(i));
            sb.append(' ').append(pad(s, widths[i])).append(' ').append('|');
        }
        return sb.toString();
    }

    private static String pad(String s, int width) {
        if (s.length() >= width) return s;
        StringBuilder sb = new StringBuilder(width);
        sb.append(s);
        for (int i = s.length(); i < width; i++) sb.append(' ');
        return sb.toString();
    }
}
