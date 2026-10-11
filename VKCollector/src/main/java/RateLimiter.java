import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple thread-safe rate limiter based on sleeping between acquisitions.
 */
public class RateLimiter {
    private final double intervalSeconds;
    private final ReentrantLock lock = new ReentrantLock();
    private long lastNanos = 0L;

    public RateLimiter(double requestsPerSecond) {
        double rps = Math.max(0.1, requestsPerSecond);
        this.intervalSeconds = 1.0 / rps;
    }

    public void acquire() {
        lock.lock();
        try {
            long now = System.nanoTime();
            long intervalNanos = (long) (intervalSeconds * 1_000_000_000L);
            long elapsed = now - lastNanos;
            long waitNanos = intervalNanos - elapsed;
            if (waitNanos > 0) {
                try {
                    long millis = waitNanos / 1_000_000L;
                    int nanos = (int) (waitNanos % 1_000_000L);
                    Thread.sleep(millis, nanos);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            lastNanos = System.nanoTime();
        } finally {
            lock.unlock();
        }
    }
}


