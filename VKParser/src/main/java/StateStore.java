import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class StateStore {
    private static final Logger LOG = LoggerFactory.getLogger(StateStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Map<String, Object> load(Path statePath) {
        try {
            if (Files.exists(statePath)) {
                byte[] bytes = Files.readAllBytes(statePath);
                return MAPPER.readValue(bytes, new TypeReference<Map<String, Object>>() {});
            }
        } catch (Exception e) {
            LOG.warn("Failed to load state from {}: {}", statePath, e.toString());
        }
        return new HashMap<>();
    }

    public static void save(Path statePath, Map<String, Object> state) {
        try {
            if (statePath.getParent() != null && !Files.exists(statePath.getParent())) {
                Files.createDirectories(statePath.getParent());
            }
            Path tmp = statePath.resolveSibling(statePath.getFileName().toString() + ".tmp");
            byte[] bytes = MAPPER.writeValueAsBytes(state);
            Files.write(tmp, bytes);
            Files.move(tmp, statePath, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            LOG.error("Failed to save state to {}: {}", statePath, e.toString());
        }
    }
}


