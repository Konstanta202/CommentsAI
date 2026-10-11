import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Обходит паблики и пишет сырые ответы VK API в JSONL (одна запись — одна строка):
 *   groups.jsonl                 — информация о пабликах (groups.getById)
 *   <screen_name>_posts.jsonl    — посты (wall.get)
 *   <screen_name>_comments.jsonl — комментарии и ответы на них (wall.getComments)
 */
class Collector {
    private static final Logger LOG = LoggerFactory.getLogger(Collector.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ZoneId ZONE = ZoneId.of("Europe/Moscow");
    private static final int PAGE = 100;
    /** Сколько ответов VK отдаёт вместе с комментарием верхнего уровня (максимум API). */
    private static final int THREAD_PREVIEW = 10;

    private final VkApiClient client;
    private final Path runDir;
    private final LocalDate since;
    private final LocalDate until;
    private final Integer maxPosts;
    private final boolean withComments;

    Collector(VkApiClient client, Path outDir, LocalDate since, LocalDate until, Integer maxPosts, boolean withComments) {
        this.client = client;
        this.runDir = outDir.resolve("run_" + DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(LocalDateTime.now()));
        this.since = since;
        this.until = until;
        this.maxPosts = maxPosts;
        this.withComments = withComments;
    }

    void run(List<String> groups) throws IOException {
        Files.createDirectories(runDir);
        LOG.info("Сохраняем в {}", runDir.toAbsolutePath());

        try (BufferedWriter groupsOut = writer("groups.jsonl")) {
            for (String g : groups) {
                JsonNode group;
                try {
                    group = fetchGroup(g.trim());
                } catch (VkApiClient.VkApiException e) {
                    LOG.warn("Пропускаем '{}': {}", g, e.getMessage());
                    continue;
                }
                ((ObjectNode) group).put("_collected_at", Instant.now().toString());
                writeLine(groupsOut, group);

                if (group.path("is_closed").asInt(0) != 0) {
                    LOG.warn("Паблик '{}' закрытый, стену прочитать нельзя", g);
                    continue;
                }
                int id = group.path("id").asInt();
                collectGroup(-id, group.path("screen_name").asText("club" + id));
            }
        }
        LOG.info("Готово: {}", runDir.toAbsolutePath());
    }

    private JsonNode fetchGroup(String raw) throws VkApiClient.VkApiException {
        String id = raw.replaceFirst("^https?://", "").replaceFirst("^(m\\.)?vk\\.(com|ru)/", "").replaceFirst("^@", "");
        JsonNode resp = client.call("groups.getById", Map.of("group_id", id, "fields", "members_count,description,city"));
        JsonNode list = resp.has("groups") ? resp.get("groups") : resp;
        if (!list.isArray() || list.isEmpty()) throw new VkApiClient.VkApiException(-1, "паблик не найден");
        return list.get(0);
    }

    private void collectGroup(int ownerId, String name) throws IOException {
        int posts = 0, comments = 0;
        try (BufferedWriter postsOut = writer(name + "_posts.jsonl");
             BufferedWriter commentsOut = withComments ? writer(name + "_comments.jsonl") : null) {

            int offset = 0;
            outer:
            while (true) {
                JsonNode items = call("wall.get", Map.of("owner_id", ownerId, "count", PAGE, "offset", offset)).path("items");
                if (!items.isArray() || items.isEmpty()) break;

                for (JsonNode post : items) {
                    LocalDate day = Instant.ofEpochSecond(post.path("date").asLong()).atZone(ZONE).toLocalDate();
                    boolean pinned = post.path("is_pinned").asInt(0) == 1;
                    // Закреплённый пост может быть старым — по нему нельзя останавливать обход
                    if (since != null && day.isBefore(since)) {
                        if (pinned) continue;
                        break outer;
                    }
                    if (until != null && day.isAfter(until)) continue;

                    writeLine(postsOut, post);
                    posts++;
                    if (commentsOut != null && post.path("comments").path("count").asInt(0) > 0) {
                        comments += collectComments(ownerId, post.path("id").asInt(), commentsOut);
                    }
                    if (maxPosts != null && posts >= maxPosts) break outer;
                }
                if (items.size() < PAGE) break;
                offset += PAGE;
                LOG.info("{}: постов {}, комментариев {}", name, posts, comments);
            }
        }
        LOG.info("{}: итого постов {}, комментариев {}", name, posts, comments);
    }

    /** Комментарии верхнего уровня; если в ветке ответов больше, чем VK отдал сразу, — догружаем ветку целиком. */
    private int collectComments(int ownerId, int postId, BufferedWriter out) throws IOException {
        int n = 0, offset = 0;
        while (true) {
            JsonNode items = call("wall.getComments", Map.of(
                    "owner_id", ownerId, "post_id", postId,
                    "count", PAGE, "offset", offset, "sort", "asc",
                    "need_likes", 1, "thread_items_count", THREAD_PREVIEW)).path("items");
            if (!items.isArray() || items.isEmpty()) break;

            for (JsonNode c : items) {
                JsonNode thread = c.path("thread");
                if (thread.path("count").asInt(0) > thread.path("items").size()) {
                    ((ObjectNode) thread).set("items", fetchThread(ownerId, postId, c.path("id").asInt()));
                }
                writeLine(out, c);
                n += 1 + thread.path("items").size();
            }
            if (items.size() < PAGE) break;
            offset += PAGE;
        }
        return n;
    }

    private JsonNode fetchThread(int ownerId, int postId, int commentId) {
        var all = MAPPER.createArrayNode();
        int offset = 0;
        while (true) {
            JsonNode items = call("wall.getComments", Map.of(
                    "owner_id", ownerId, "post_id", postId, "comment_id", commentId,
                    "count", PAGE, "offset", offset, "sort", "asc", "need_likes", 1)).path("items");
            if (!items.isArray() || items.isEmpty()) break;
            all.addAll((ArrayNode) items);
            if (items.size() < PAGE) break;
            offset += PAGE;
        }
        return all;
    }

    /** Ошибка на одном запросе (например, комментарии закрыты) не роняет весь сбор. */
    private JsonNode call(String method, Map<String, Object> params) {
        try {
            return client.call(method, params);
        } catch (VkApiClient.VkApiException e) {
            LOG.warn("{} {}: {}", method, params, e.getMessage());
            return MAPPER.createObjectNode();
        }
    }

    private BufferedWriter writer(String file) throws IOException {
        return Files.newBufferedWriter(runDir.resolve(file), StandardCharsets.UTF_8);
    }

    private static void writeLine(BufferedWriter w, JsonNode node) throws IOException {
        w.write(MAPPER.writeValueAsString(node));
        w.write('\n');
    }
}
