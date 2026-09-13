import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JsonlIO {
    private static final Logger LOG = LoggerFactory.getLogger(JsonlIO.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    public static int write(Path path, Iterable<Map<String, Object>> records) throws IOException {
        ensureDir(path.getParent());
        int n = 0;
        try (BufferedWriter w = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            for (Map<String, Object> rec : records) {
                w.write(MAPPER.writeValueAsString(rec));
                w.write('\n');
                n++;
                if (n % 1000 == 0) {
                    LOG.info("Wrote {} records to {}", n, path);
                }
            }
        }
        LOG.info("Finished writing {} records to {}", n, path);
        return n;
    }

    public static int append(Path path, Iterable<Map<String, Object>> records) throws IOException {
        ensureDir(path.getParent());
        int n = 0;
        try (BufferedWriter w = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND)) {
            for (Map<String, Object> rec : records) {
                w.write(MAPPER.writeValueAsString(rec));
                w.write('\n');
                n++;
                if (n % 1000 == 0) {
                    LOG.info("Appended {} records to {}", n, path);
                }
            }
        }
        if (n > 0) {
            LOG.info("Appended {} records to {}", n, path);
        }
        return n;
    }

    public static int appendWithCallback(Path path, Iterable<Map<String, Object>> records, RecordCallback cb) throws IOException {
        ensureDir(path.getParent());
        int n = 0;
        try (BufferedWriter w = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND)) {
            for (Map<String, Object> rec : records) {
                w.write(MAPPER.writeValueAsString(rec));
                w.write('\n');
                if (cb != null) {
                    try {
                        cb.onRecord(rec);
                    } catch (Exception e) {
                        LOG.error("onRecord callback failed", e);
                    }
                }
                n++;
                if (n % 1000 == 0) {
                    LOG.info("Appended {} records to {}", n, path);
                }
            }
        }
        if (n > 0) {
            LOG.info("Appended {} records to {}", n, path);
        }
        return n;
    }

    public interface RecordCallback {
        void onRecord(Map<String, Object> record);
    }

    private static void ensureDir(Path dir) throws IOException {
        if (dir != null && !Files.exists(dir)) {
            Files.createDirectories(dir);
        }
    }
}


