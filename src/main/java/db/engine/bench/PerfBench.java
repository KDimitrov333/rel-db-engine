package db.engine.bench;

import db.engine.catalog.CatalogManager;
import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.catalog.TableSchema;
import db.engine.index.IndexManager;
import db.engine.query.QueryProcessor;
import db.engine.storage.Record;
import db.engine.storage.StorageManager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.Iterator;

public class PerfBench {
    private static final String STUDENTS = "bench_students";
    private static final String ENROLLMENTS = "bench_enrollments";
    public static void main(String[] args) throws Exception {
        Path root = Paths.get("benchdata");
        Files.createDirectories(root);
        System.out.println("Benchmark root: " + root.toAbsolutePath());

        // Parse CLI overrides for seed sizes and options.
        BenchmarkConfig cfg = BenchmarkConfig.fromArgs(root, args);

        Path dataDir = root.resolve("data");
        Files.createDirectories(dataDir);

        CatalogManager catalog = new CatalogManager();
        StorageManager storage = new StorageManager(catalog);
        IndexManager index = new IndexManager(catalog, storage);

        // Seed tables if missing; use --reseed to force fresh data (delete existing .tbl files).
        boolean forceReseed = false;
        for (String a : args) { if ("--reseed".equals(a)) { forceReseed = true; break; } }

        if (forceReseed) {
            // Proactively remove existing benchmark table files to avoid appending/skew.
            Path studentsTbl = dataDir.resolve(STUDENTS + ".tbl");
            Path enrollmentsTbl = dataDir.resolve(ENROLLMENTS + ".tbl");
            try {
                java.nio.file.Files.deleteIfExists(studentsTbl);
                java.nio.file.Files.deleteIfExists(enrollmentsTbl);
                System.out.println("[reseed] Deleted existing table files: " + studentsTbl + ", " + enrollmentsTbl);
            } catch (Exception e) {
                System.err.println("[reseed] Warning: failed to delete table files: " + e.getMessage());
            }
        }
        boolean studentsExists = catalog.getTableSchema(STUDENTS) != null && fileExists(catalog.getTableSchema(STUDENTS));
        boolean enrollmentsExists = catalog.getTableSchema(ENROLLMENTS) != null && fileExists(catalog.getTableSchema(ENROLLMENTS));

        if (forceReseed || !studentsExists || !enrollmentsExists) {
            System.out.println("Seeding students=" + cfg.rowsStudents + ", enrollments=" + cfg.rowsEnrollments);
            // ID pool size matches students to scale with dataset
            DataPool idPool = new DataPool((int) cfg.rowsStudents, cfg.seed);
            NamePool names = new NamePool();
            seedStudents(catalog, storage, dataDir, (int) cfg.rowsStudents, idPool, cfg.useNames ? names : null, STUDENTS);
            seedEnrollments(catalog, storage, dataDir, (int) cfg.rowsEnrollments, idPool, ENROLLMENTS);
        } else {
            System.out.println("Using existing bench tables under: " + dataDir.toAbsolutePath());
        }

        // Build index on students(id) used by equality/range queries
        index.createIndex("bench_students_id_idx", STUDENTS, "id");
        QueryProcessor qp = new QueryProcessor(catalog, storage, index);

        Map<String, StatsAggregator> stats = new LinkedHashMap<>();
        Map<String, StatsAggregator> rowStats = new LinkedHashMap<>();
        Map<String, String> sqlTemplates = new LinkedHashMap<>();
        Map<String, String> descriptions = Map.of(
            "scan", "Full table scan of students",
            "equality_hit", "Indexed equality lookup for existing id",
            "equality_seq", "Sequential equality lookup for existing name (non-indexed VARCHAR)",
            "equality_miss", "Non-indexed equality lookup for non-existing name",
            "range", "Indexed range query on id",
            "join", "Inner join students ↔ enrollments on id = student_id"
        );
        DataPool idPool = new DataPool((int) cfg.rowsStudents, cfg.seed);
        // Collect existing ids/names to ensure hit queries use present literals
        ArrayList<String> existingNames = new ArrayList<>();
        ArrayList<Integer> existingIds = new ArrayList<>();
        storage.scan(STUDENTS, (rid, rec) -> {
            Object idv = rec.getValues().get(0);
            if (idv instanceof Integer) existingIds.add((Integer) idv);
            Object nv = rec.getValues().get(1);
            if (nv instanceof String) existingNames.add((String) nv);
        });
        // Use time-based RNG per run for varied samples
        Random literalRng = new Random(System.nanoTime());

        // Pre-sample literals: shuffle and take 'runs' items for guaranteed hits
        List<Integer> shuffledIds = new ArrayList<>(existingIds);
        List<String> shuffledNames = new ArrayList<>(existingNames);
        Collections.shuffle(shuffledIds, literalRng);
        Collections.shuffle(shuffledNames, literalRng);
        List<Integer> sampledIds = new ArrayList<>(shuffledIds.subList(0, Math.min(cfg.runs, shuffledIds.size())));
        List<String> sampledNames = new ArrayList<>(shuffledNames.subList(0, Math.min(cfg.runs, shuffledNames.size())));
        for (String q : cfg.queries) {
            StatsAggregator agg = new StatsAggregator();
            StatsAggregator rowsAgg = new StatsAggregator();
            for (int i = 0; i < cfg.warmup; i++) runQueryOnce(qp, storage, q, idPool, existingNames, literalRng, 0, sampledIds, sampledNames);
            for (int i = 0; i < cfg.runs; i++) {
                long t0 = System.nanoTime();
                RunResult result = runQueryOnce(qp, storage, q, idPool, existingNames, literalRng, i, sampledIds, sampledNames);
                long t1 = System.nanoTime();
                agg.add(t1 - t0);
                rowsAgg.add(result.rows);
                // Store template only
                if (!sqlTemplates.containsKey(q)) sqlTemplates.put(q, result.templateSql);
            }
            stats.put(q, agg);
            rowStats.put(q, rowsAgg);
                // Print in both ms and µs to avoid 0.000ms for ultra-fast ops
                double meanNs = agg.mean();
                double medNs = agg.median();
                double minNs = agg.min();
                double maxNs = agg.max();
                double meanMs = meanNs / 1_000_000.0;
                double medianMs = medNs / 1_000_000.0;
                double minMs = minNs / 1_000_000.0;
                double maxMs = maxNs / 1_000_000.0;
                double medianUs = medNs / 1_000.0;
                double minUs = minNs / 1_000.0;
                double maxUs = maxNs / 1_000.0;
                System.out.printf(Locale.ROOT,
                    "%s -> count=%d mean=%.2fms median=%.3fms (%.1fµs) min=%.3fms (%.1fµs) max=%.3fms (%.1fµs)\n",
                    q, agg.count(), meanMs, medianMs, medianUs, minMs, minUs, maxMs, maxUs);
        }

        ReportWriter writer = new ReportWriter(root.resolve("results"));
        writer.writeJson(stats, rowStats, descriptions, sqlTemplates, cfg);
        System.out.println("Wrote JSON to: " + root.resolve("results").toAbsolutePath());
    }

    private static boolean fileExists(TableSchema ts) {
        if (ts == null) return false;
        return new File(ts.filePath()).exists();
    }

    private static void seedStudents(CatalogManager catalog,
                                     StorageManager storage,
                                     Path dataDir,
                                     int count,
                                     DataPool pool,
                                     NamePool names,
                                     String tableName) throws Exception {
        TableSchema existing = catalog.getTableSchema(tableName);
        String studentPath = dataDir.resolve(tableName + ".tbl").toString();
        if (existing == null) {
            TableSchema students = new TableSchema(
                    tableName,
                    List.of(
                            new ColumnSchema("id", DataType.INT, 0),
                            new ColumnSchema("name", DataType.VARCHAR, 64)
                    ),
                    studentPath
            );
            storage.createTable(students);
        } else {
            // Recreate file for fresh seed
            File f = new File(existing.filePath());
            if (f.exists()) f.delete();
            try {
                f.getParentFile().mkdirs();
                f.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException("Failed recreating students file", e);
            }
        }

        final int LOG_INTERVAL = Math.max(1, count / 10);
        for (int i = 1; i <= count; i++) {
            int id = pool.randomId();
            String name = names != null ? names.randomFullName() : "Name" + i;
            List<Object> vals = List.of(id, name);
            storage.insert(tableName, new Record(vals));
            if (i % LOG_INTERVAL == 0) {
                System.out.println("Seeded students: " + i + "/" + count);
            }
        }
    }

    private static void seedEnrollments(CatalogManager catalog,
                                        StorageManager storage,
                                        Path dataDir,
                                        int count,
                                        DataPool pool,
                                        String tableName) throws Exception {
        TableSchema existing = catalog.getTableSchema(tableName);
        String enrollPath = dataDir.resolve(tableName + ".tbl").toString();
        if (existing == null) {
            TableSchema enrollments = new TableSchema(
                    tableName,
                    List.of(
                            new ColumnSchema("student_id", DataType.INT, 0),
                            new ColumnSchema("course", DataType.VARCHAR, 32)
                    ),
                    enrollPath
            );
            storage.createTable(enrollments);
        } else {
            File f = new File(existing.filePath());
            if (f.exists()) f.delete();
            try {
                f.getParentFile().mkdirs();
                f.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException("Failed recreating enrollments file", e);
            }
        }

        final int LOG_INTERVAL = Math.max(1, count / 10);
        for (int i = 1; i <= count; i++) {
            int sid = pool.randomId();
            String course = "C" + ((i % 200) + 1);
            List<Object> vals = List.of(sid, course);
            storage.insert(tableName, new Record(vals));
            if (i % LOG_INTERVAL == 0) {
                System.out.println("Seeded enrollments: " + i + "/" + count);
            }
        }
    }

    private static RunResult runQueryOnce(QueryProcessor qp, StorageManager storage, String q, DataPool pool,
                                          List<String> names, Random rng,
                                          int runIndex, List<Integer> sampledIds,
                                          List<String> sampledNames) throws Exception {
        String sql;
        String template;
        int count;
        switch (q) {
            case "scan":
                sql = "SELECT * FROM " + STUDENTS;
                template = "SELECT * FROM " + STUDENTS;
                count = countRows(qp.execute(sql));
                break;
            case "equality_hit": {
                // Guaranteed-hit id
                int attempts = 0;
                do {
                    int existing = sampledIds.get((runIndex + attempts) % sampledIds.size());
                    sql = "SELECT * FROM " + STUDENTS + " WHERE id = " + existing;
                    count = countRows(qp.execute(sql));
                    attempts++;
                } while (count == 0 && attempts < 10);
                template = "SELECT * FROM " + STUDENTS + " WHERE id = ?";
                break;
            }
            case "equality_seq": {
                // Guaranteed-hit name (non-indexed)
                int attempts = 0;
                do {
                    String name = sampledNames.get((runIndex + attempts) % sampledNames.size());
                    String lit = name.replace("'", "''");
                    sql = "SELECT * FROM " + STUDENTS + " WHERE name = '" + lit + "'";
                    count = countRows(qp.execute(sql));
                    attempts++;
                } while (count == 0 && attempts < 10);
                template = "SELECT * FROM " + STUDENTS + " WHERE name = ?";
                break;
            }
            case "equality_miss": {
                String missing = "__NO_SUCH_NAME__";
                sql = "SELECT * FROM " + STUDENTS + " WHERE name = '" + missing + "'";
                template = "SELECT * FROM " + STUDENTS + " WHERE name = ?";
                count = countRows(qp.execute(sql));
                break;
            }
            case "range": {
                int attempts = 0;
                int[] widths = {25, 200, 1000, 5000};
                do {
                    int center = sampledIds.get((runIndex + attempts) % sampledIds.size());
                    int delta = widths[rng.nextInt(widths.length)];
                    int lo = Math.max(0, center - delta);
                    int hi = center + delta;
                    sql = "SELECT * FROM " + STUDENTS + " WHERE id >= " + lo + " AND id <= " + hi;
                    count = countRows(qp.execute(sql));
                    attempts++;
                } while (count == 0 && attempts < 10);
                template = "SELECT * FROM " + STUDENTS + " WHERE id >= ? AND id <= ?";
                break;
            }
            case "join":
                sql = "SELECT * FROM " + STUDENTS + " JOIN " + ENROLLMENTS + " ON id = student_id";
                template = "SELECT * FROM " + STUDENTS + " JOIN " + ENROLLMENTS + " ON id = student_id";
                count = countRows(qp.execute(sql));
                break;
            default:
                return new RunResult(0, "");
        }
            return new RunResult(count, template);
    }

    private static int countRows(Iterable<db.engine.exec.Row> rows) {
        int c = 0;
        Iterator<?> it = rows.iterator();
        while (it.hasNext()) { it.next(); c++; }
        return c;
    }

    // Results per run (row count + template SQL)

    private static final class RunResult {
        final int rows;
        final String templateSql;
        RunResult(int rows, String templateSql) { this.rows = rows; this.templateSql = templateSql; }
    }
}
