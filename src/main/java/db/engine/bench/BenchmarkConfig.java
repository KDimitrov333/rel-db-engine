package db.engine.bench;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class BenchmarkConfig {
    public final long rowsStudents;
    public final long rowsEnrollments;
    public final int idPoolSize;
    public final int runs;
    public final int warmup;
    public final long seed;
    public final boolean useNames;
    public final Path benchRoot;
    public final List<String> queries;

    public BenchmarkConfig(long rowsStudents,
                           long rowsEnrollments,
                           int idPoolSize,
                           int runs,
                           int warmup,
                           long seed,
                           boolean useNames,
                           Path benchRoot,
                           List<String> queries) {
        this.rowsStudents = rowsStudents;
        this.rowsEnrollments = rowsEnrollments;
        this.idPoolSize = idPoolSize;
        this.runs = runs;
        this.warmup = warmup;
        this.seed = seed;
        this.useNames = useNames;
        this.benchRoot = benchRoot;
        this.queries = queries;
    }

    public static BenchmarkConfig defaultConfig(Path benchRoot) {
        long rowsStudents = 10_000; // default students
        long rowsEnrollments = Math.max(1, rowsStudents / 10); // 0.1x enrollments
        int idPoolSize = (int) rowsStudents; // ID pool scales with students
        return new BenchmarkConfig(
                rowsStudents,
                rowsEnrollments,
                idPoolSize,
                100,      // runs per query
                5,       // warmup per query
                42L,     // random seed
                true,    // use names
                benchRoot,
                Arrays.asList("scan", "equality_hit", "equality_seq", "equality_miss", "range", "join")
        );
    }

    public static BenchmarkConfig fromArgs(Path benchRoot, String[] args) {
        long rowsStudents = 10_000;
        long rowsEnrollments = Math.max(1, rowsStudents / 10);
        int runs = 100;
        int warmup = 5;
        long seed = 42L;
        boolean useNames = true;

        for (String a : args) {
            if (a == null) continue;
            String s = a.trim();
            if (s.startsWith("--rows=")) {
                try { rowsStudents = Long.parseLong(s.substring("--rows=".length())); } catch (NumberFormatException ignored) {}
            } else if (s.startsWith("--runs=")) {
                try { runs = Integer.parseInt(s.substring("--runs=".length())); } catch (NumberFormatException ignored) {}
            } else if (s.startsWith("--warmup=")) {
                try { warmup = Integer.parseInt(s.substring("--warmup=".length())); } catch (NumberFormatException ignored) {}
            } else if (s.startsWith("--seed=")) {
                try { seed = Long.parseLong(s.substring("--seed=".length())); } catch (NumberFormatException ignored) {}
            } else if (s.equals("--no-names")) {
                useNames = false;
            }
        }

        // Compute enrollments as 0.1x of students after parsing
        rowsEnrollments = Math.max(1, rowsStudents / 10);
        int idPoolSize = (int) rowsStudents;
        return new BenchmarkConfig(
                rowsStudents,
                rowsEnrollments,
                idPoolSize,
                runs,
                warmup,
                seed,
                useNames,
                benchRoot,
                Arrays.asList("scan", "equality_hit", "equality_seq", "equality_miss", "range", "join")
        );
    }
}
