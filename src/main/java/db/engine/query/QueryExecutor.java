package db.engine.query;

import java.util.Iterator;
import java.util.NoSuchElementException;

import db.engine.exec.Operator;
import db.engine.exec.Row;

/**
 * Executes a planned operator pipeline via streaming
 */
public class QueryExecutor {
    /**
     * Streaming interface: returns an Iterable that opens the operator on first iteration
     * and closes it when exhausted.
     * This pulls rows one at a time and avoids building a large intermediate list.
     */
    public Iterable<Row> stream(Operator op) {
        return () -> new Iterator<Row>() {
            private boolean opened = false;
            private Row next = null;
            private boolean finished = false;

            private void ensureOpen() {
                if (!opened) {
                    op.open();
                    opened = true;
                    advance();
                }
            }

            private void advance() {
                if (finished) return;
                next = op.next();
                if (next == null) {
                    finished = true;
                    op.close();
                }
            }

            @Override
            public boolean hasNext() {
                ensureOpen();
                return !finished;
            }

            @Override
            public Row next() {
                if (!hasNext()) throw new NoSuchElementException();
                Row current = next;
                advance();
                return current;
            }
        };
    }
}
