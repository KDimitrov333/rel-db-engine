package db.engine.query;

import java.util.ArrayList;
import java.util.List;

import db.engine.exec.Operator;
import db.engine.exec.Row;

/**
 * Executes a planned operator pipeline, collecting rows into a list.
 */
public class QueryExecutor {
    public List<Row> execute(Operator op) {
        op.open();
        try {
            List<Row> out = new ArrayList<>();
            Row r;
            while ((r = op.next()) != null) {
                out.add(r);
            }
            return out;
        } finally {
            op.close();
        }
    }
}
