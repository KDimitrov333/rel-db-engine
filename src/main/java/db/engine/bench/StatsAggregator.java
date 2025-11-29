package db.engine.bench;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StatsAggregator {
    private final List<Long> samples = new ArrayList<>();

    public void add(long nanos) {
        samples.add(nanos);
    }

    public int count() { return samples.size(); }

    public long min() { return samples.isEmpty() ? 0 : Collections.min(samples); }
    public long max() { return samples.isEmpty() ? 0 : Collections.max(samples); }

    public double mean() {
        if (samples.isEmpty()) return 0.0;
        long sum = 0L;
        for (long s : samples) sum += s;
        return (double) sum / samples.size();
    }
    
    // Sample variance (unbiased, n-1 denominator) in nanoseconds^2
    public double variance() {
        int n = samples.size();
        if (n < 2) return 0.0;
        double m = mean();
        double acc = 0.0;
        for (long v : samples) {
            double diff = v - m;
            acc += diff * diff;
        }
        return acc / (n - 1);
    }
    
    // Standard deviation (sqrt of sample variance) in nanoseconds
    public double stddev() {
        return Math.sqrt(variance());
    }

    public long median() {
        if (samples.isEmpty()) return 0L;
        List<Long> copy = new ArrayList<>(samples);
        Collections.sort(copy);
        int mid = copy.size() / 2;
        if (copy.size() % 2 == 0) {
            return (copy.get(mid - 1) + copy.get(mid)) / 2;
        } else {
            return copy.get(mid);
        }
    }
}
