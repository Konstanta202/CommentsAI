import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

class VkApiClient {
    private static final Logger LOG = LoggerFactory.getLogger(VkApiClient.class);
    private static final String API_VERSION = "5.199";
    private static final String API_BASE_URL = "https://api.vk.com/method/";

    private final String token;
    private final HttpClient http;
    private final ObjectMapper mapper = new ObjectMapper();
    private final RateLimiter rateLimiter;

    public VkApiClient(String token, double requestsPerSecond, int httpPool) {
        this.token = token;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        this.rateLimiter = new RateLimiter(requestsPerSecond);
    }

    public JsonNode call(String method, Map<String, Object> params) throws VkApiException {
        return call(method, params, 5, 350);
    }

    public JsonNode call(String method, Map<String, Object> params, int maxRetries, long baseDelayMs) throws VkApiException {
        Map<String, Object> merged = new LinkedHashMap<>(params);
        merged.put("access_token", token);
        merged.put("v", API_VERSION);

        VkApiException lastError = null;
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            rateLimiter.acquire();
            try {
                String url = API_BASE_URL + method + "?" + encodeParams(merged);
                HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                        .timeout(Duration.ofSeconds(30))
                        .GET()
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                    JsonNode node = mapper.readTree(resp.body());
                    if (node.has("error")) {
                        JsonNode err = node.get("error");
                        int code = err.path("error_code").asInt(-1);
                        String msg = err.path("error_msg").asText("Unknown error");
                        lastError = new VkApiException(code, msg);
                        if (isRetryable(code)) {
                            long sleep = (long) (baseDelayMs * Math.pow(2, attempt));
                            LOG.warn("VK API error on {} attempt {}: code={} msg={}. Retry in {} ms", method, attempt + 1, code, msg, sleep);
                            sleep(sleep);
                            continue;
                        } else {
                            throw lastError;
                        }
                    }
                    return node.get("response");
                } else {
                    lastError = new VkApiException(-1, "HTTP " + resp.statusCode());
                }
            } catch (IOException | InterruptedException e) {
                lastError = new VkApiException(-1, "HTTP error: " + e.getMessage());
            }

            long sleep = (long) (baseDelayMs * Math.pow(2, attempt));
            LOG.warn("HTTP/API error on {} attempt {}. Backoff {} ms", method, attempt + 1, sleep);
            sleep(sleep);
        }
        if (lastError != null) throw lastError;
        throw new VkApiException(-1, "Unknown VK API failure");
    }

    private static boolean isRetryable(int code) {
        return code == 1 || code == 6 || code == 9 || code == 10;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String encodeParams(Map<String, Object> params) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, Object> e : params.entrySet()) {
            if (!first) sb.append('&');
            first = false;
            sb.append(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8));
            sb.append('=');
            sb.append(URLEncoder.encode(String.valueOf(e.getValue()), StandardCharsets.UTF_8));
        }
        return sb.toString();
    }

    public static class VkApiException extends Exception {
        public final int code;
        public VkApiException(int code, String message) {
            super("VK API error " + code + ": " + message);
            this.code = code;
        }
    }
}


