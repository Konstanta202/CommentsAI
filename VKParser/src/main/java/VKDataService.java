
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class VKDataService {

    private static final Logger LOG = LoggerFactory.getLogger(VKDataService.class);

    // private final DatabaseService dbService;

    // public VkDataService(DatabaseService dbService) {
    //     this.dbService = dbService;
    // }

    static void savePostToDatabase(JsonNode post, int ownerId, DatabaseService dbService) {
        try {
            
            Long groupId = (long) Math.abs(ownerId); // owner_id отрицательный для групп
            Long postId = post.path("id").asLong();
            Long commentsCount = post.path("comments").path("count").asLong();
            Long date = post.path("date").asLong();
            
            Timestamp createdAt = new Timestamp(date * 1000); // Конвертируем Unix timestamp в Timestamp
            
            dbService.savePost(groupId, postId, commentsCount, createdAt);
            
        } catch (Exception e) {
            LOG.error("Error saving post to database: {}", e.getMessage());
        }
    }

    static List<Map<String, Object>> collectGroupsInfo(VkApiClient client, List<Integer> ownerIds, DatabaseService dbService) {
        List<Map<String, Object>> groupsInfo = new ArrayList<>();
        
        List<Integer> groupIds = ownerIds.stream()
                .filter(id -> id < 0)
                .map(id -> -id)
                .toList();

        if (groupIds.isEmpty()) return groupsInfo;

        for (int groupId : groupIds) {
            try {
                JsonNode resp = client.call("groups.getById",
                    Map.of( "group_id",
                            groupId,
                            "fields",
                            "members_count,name,screen_name,type,description"));                

                LOG.debug("Response for group {}: {}", groupId, resp.toString());

                // Map<String, Object> groupInfo = JsonUtil.toMap(resp);
                // groupInfo.put("_collected_at", Instant.now().toString());
                // groupsInfo.add(groupInfo);
                if (resp.has("groups") && resp.get("groups").isArray()) {

                    for (JsonNode groupData: resp.get("groups")) {

                        Long groupIdLong = (long) groupId;
                        String name = groupData.path("name").asText();
                        Long membersCount = groupData.path("members_count").asLong();
                        
                        dbService.saveGroup(groupIdLong, name, membersCount);
                        
                        Map<String, Object> groupInfo = JsonUtil.toMap(groupData);
                        groupInfo.put("_collected_at", Instant.now().toString());
                        groupsInfo.add(groupInfo);
                    }

                }

            } catch (Exception e) {
                LOG.error("Failed for group {}: {}", groupId, e.getMessage());
            }
        }

        return groupsInfo;
    }

    static void fetchCommentsAndSaveToDatabase(VkApiClient client, int ownerId, List<Map<String, Object>> posts, int parallelComments, DatabaseService dbService) throws Exception {
            var pool = Executors.newFixedThreadPool(Math.max(1, parallelComments));
            List<Future<Integer>> futures = new ArrayList<>();
            
            for (Map<String, Object> post : posts) {
                int postId = ((Number) post.get("id")).intValue();
                Map<?, ?> commentsMap = (Map<?, ?>) post.get("comments");
                int commentsCount = 0;
                if (commentsMap != null) {
                    Object cc = commentsMap.get("count");
                    if (cc instanceof Number) {
                        commentsCount = ((Number) cc).intValue();
                    }
                }
                if (commentsCount <= 0) {
                    LOG.debug("Post {} has no comments. Skip.", postId);
                    continue;
                }
                
                futures.add(pool.submit(() -> 
                    fetchCommentsForPostAndSaveToDatabase(client, ownerId, postId, dbService)
                ));
            }
            
            int totalComments = 0;
            for (Future<Integer> f : futures) {
                try { 
                    totalComments += f.get(); 
                } catch (Exception e) { 
                    LOG.error("Error fetching comments: {}", e.toString()); 
                }
            }
            pool.shutdown();
            
            LOG.info("Total comments saved to database: {}", totalComments);
    }

    static void saveCommentsToDatabase(VkApiClient client, List<Map<String, Object>> comments, DatabaseService dbService, int ownerId) {
        int savedCount = 0;
        LOG.info("Start add new comments to database");
        for (Map<String, Object> comment : comments) {
            try {
                int postId = ((Number) comment.get("_post_id")).intValue();
                Long userId = ((Number) comment.get("from_id")).longValue();
                String commentText = (String) comment.get("text");
                Long date = ((Number) comment.get("date")).longValue();
                
                Long groupId = (long) Math.abs(ownerId);
                
                Optional<Long> postsIdOpt = dbService.findPostsIdByPostAndGroupId((long) postId, groupId);
                
                if (postsIdOpt.isEmpty()) {
                    LOG.warn("Post not found in database: post_id={}, group_id={}", postId, groupId);
                    continue;
                }
                
                Long postsId = postsIdOpt.get();
                Long user_id = -1L;
                
                if (userId > 0) {
                        Optional<Long> userIdOpt = dbService.findUserIdByVkId(userId);
                        if (userIdOpt.isEmpty()){
                            try {
                                JsonNode user = parseUser(client, userId);
                                userSave(user, dbService);

                                userIdOpt = dbService.findUserIdByVkId(userId);
                            } catch (Exception e) {
                                LOG.warn("Failed to save user {}: {}", userId, e.getMessage());
                                continue;
                            }
                        }
                        user_id = userIdOpt.get();
                }

                Timestamp createdAt = new Timestamp(date * 1000);
                
                // Сохраняем в БД (ИДЕНТИЧНО оригинальному методу)
                dbService.saveComment(postsId, user_id, commentText, createdAt);
                savedCount++;
                LOG.info("SAVIDDDDD {} comment for post {} to database", savedCount, postId);
            } catch (Exception e) {
                LOG.error("Error saving comment to database: {}", e.getMessage());
            }
        }
        
        if (savedCount > 0) {
            LOG.info("Saved {} comments to database", savedCount);
        }
    }

    static int fetchCommentsForPostAndSaveToDatabase(VkApiClient client, int ownerId, int postId, DatabaseService dbService) {
        int savedCount = 0;
        Long groupId = (long) Math.abs(ownerId);
        
        try {
            Optional<Long> postsIdOpt = dbService.findPostsIdByPostAndGroupId((long) postId, groupId);
            
            // ДОБАВЬТЕ ПРОВЕРКУ isPresent()
            if (postsIdOpt.isEmpty()) {
                LOG.warn("Post not found in database: post_id={}, group_id={}", postId, groupId);
                return 0;
            }
            
            Long postsId = postsIdOpt.get();
            // Set<Long> processedUsers = new HashSet<>();
            
            for (JsonNode comment : iteratePostComments(client, ownerId, postId)) {
                if (isValidComment(comment)) {
                    
                    Long userId = comment.path("from_id").asLong();
                    Long user_id = -1L;
                    if (userId > 0) {
                        Optional<Long> userIdOpt = dbService.findUserIdByVkId(userId);
                        
                        // ТАКЖЕ ДОБАВЬТЕ ПРОВЕРКУ ЗДЕСЬ
                        if (userIdOpt.isEmpty()){
                            try {
                                JsonNode user = parseUser(client, userId);
                                userSave(user, dbService);

                                userIdOpt = dbService.findUserIdByVkId(userId);
                                
                                // ЕЩЁ ОДНА ПРОВЕРКА ПОСЛЕ СОХРАНЕНИЯ ПОЛЬЗОВАТЕЛЯ
                                if (userIdOpt.isEmpty()) {
                                    LOG.warn("User {} still not found after save, skipping comment", userId);
                                    continue;
                                }
                            } catch (Exception e) {
                                LOG.warn("Failed to save user {}: {}", userId, e.getMessage());
                                continue;
                            }
                        }
                        user_id = userIdOpt.get();
                    }

                    String commentText = comment.path("text").asText();
                    Long date = comment.path("date").asLong();
                    
                    Timestamp createdAt = new Timestamp(date * 1000);
                    
                    dbService.saveComment(postsId, user_id, commentText, createdAt);
                    savedCount++;
                }
            }
            
            LOG.info("Saved {} comments for post {} to database", savedCount, postId);
            
        } catch (Exception e) {
            LOG.error("Error fetching/saving comments for post {}: {}", postId, e.getMessage());
        }
        
        return savedCount;
    }

    static void userSave(JsonNode userNode, DatabaseService dbService) {
        try {
            LOG.info("Start handler User: {}", userNode.path("id").asLong());

            Long userId = userNode.path("id").asLong();
            String firstName = userNode.path("first_name").asText(null);
            String lastName = userNode.path("last_name").asText(null);
            
            // ВАЛИДАЦИЯ: проверка на системного пользователя и обязательные поля
            if (userId == 0 || firstName == null || lastName == null) {
                LOG.warn("Skipping invalid user: ID={}, firstName={}, lastName={}", 
                        userId, firstName, lastName);
                return; // Прерываем выполнение для некорректных пользователей
            }
            
            Boolean isClosed = userNode.path("is_closed").asBoolean(false);
            
            // Парсинг страны
            String country = null;
            JsonNode countryNode = userNode.path("country");
            if (!countryNode.isMissingNode() && countryNode.has("title")) {
                country = countryNode.path("title").asText(null);
            }
            
            // Парсинг города
            String city = null;
            JsonNode cityNode = userNode.path("city");
            if (!cityNode.isMissingNode() && cityNode.has("title")) {
                city = cityNode.path("title").asText(null);
            }
            
            // Парсинг телефонов
            String mobilePhone = userNode.path("mobile_phone").asText(null);
            String homePhone = userNode.path("home_phone").asText(null);
            
            // Объединяем телефоны в одно поле
            String phoneNumber = null;
            if (mobilePhone != null && homePhone != null) {
                phoneNumber = "Мобильный: " + mobilePhone + ", Домашний: " + homePhone;
            } else if (mobilePhone != null) {
                phoneNumber = mobilePhone;
            } else if (homePhone != null) {
                phoneNumber = homePhone;
            }
            
            LOG.info("Add TO DB User: {}", userId);
            String about = userNode.path("about").asText(null);
            dbService.saveUserToDatabase(userId, firstName, lastName, isClosed, country, city, phoneNumber, about);
            
        } catch (Exception e) {
            LOG.error("Error when get user data: ", e.getMessage());
        }
    }

    // static JsonNode parseUser(VkApiClient client, Long userId) throws VkApiClient.VkApiException {
    //     String fields = "first_name,last_name,is_closed,country,city,contacts,about";
        
    //     LOG.info("GET user {}", userId);
    //     Map<String, Object> params = Map.of(
    //         "user_ids", String.valueOf(userId),
    //         "fields", fields
    //     );
        
    //     JsonNode resp = client.call("users.get", params);
    //     LOG.info("FINISH GET user");
    //     LOG.info("GET user response: {}", resp);
        
    //     // ИЗВЛЕКАЕМ ПЕРВОГО ПОЛЬЗОВАТЕЛЯ ИЗ МАССИВА
    //     if (resp.has("response") && resp.get("response").isArray() && resp.get("response").size() > 0) {
    //         JsonNode firstUser = resp.get("response").get(0);
    //         LOG.info("Extracted user: ID={}, name={} {}", 
    //             firstUser.path("id").asLong(),
    //             firstUser.path("first_name").asText(),
    //             firstUser.path("last_name").asText());
    //         return firstUser;
    //     } else {
    //         LOG.warn("No user found in response for ID: {}", userId);
    //         return null;
    //     }
    // }
    static JsonNode parseUser(VkApiClient client, Long userId) throws VkApiClient.VkApiException {
        String fields = "first_name,last_name,is_closed,country,city,contacts,about";
        
        LOG.info("GET user {}", userId);
        
        try {
            Map<String, Object> params = Map.of(
                "user_ids", String.valueOf(userId),
                "fields", fields
            );
            
            JsonNode resp = client.call("users.get", params);
            LOG.info("GET user response: {}", resp);
            
            // ОБРАБАТЫВАЕМ ОТВЕТ КАК МАССИВ НАПРЯМУЮ
            if (resp.isArray() && resp.size() > 0) {
                JsonNode firstUser = resp.get(0);
                Long foundUserId = firstUser.path("id").asLong();
                
                if (foundUserId > 0) {
                    LOG.info("Successfully found user: ID={}, name={} {}", 
                        foundUserId,
                        firstUser.path("first_name").asText(),
                        firstUser.path("last_name").asText());
                    return firstUser;
                }
            }
            
            LOG.warn("No valid user found in array response for ID: {}", userId);
            return null;
            
        } catch (Exception e) {
            LOG.error("Error parsing user {}: {}", userId, e.getMessage());
            return null;
        }
    }

    record PostCommentsResult(int postId, int maxSeenId, List<Map<String, Object>> comments) {}

    static boolean isValidComment(JsonNode c) {
        String text = c.hasNonNull("text") ? c.get("text").asText("") : c.path("message").asText("");
        if (text == null || text.trim().isEmpty()) return false;
        if (!c.has("id")) return false;
        return true;
    }


    //! Метод для runonce 
    static Iterable<JsonNode> iterateWallPosts(VkApiClient client, int ownerId) throws VkApiClient.VkApiException {
        return () -> new Iterator<JsonNode>() {
            int offset = 0;
            final int count = 100;
            Iterator<JsonNode> current = Collections.emptyIterator();
            boolean done = false;

            @Override public boolean hasNext() {
                fetchIfNeeded();
                return current.hasNext();
            }
            @Override public JsonNode next() {
                fetchIfNeeded();
                return current.next();
            }
            private void fetchIfNeeded() {
                if (done || current.hasNext()) return;
                try {
                    JsonNode resp = client.call("wall.get", Map.of("owner_id", ownerId, "count", count, "offset", offset));
                    ArrayNode items = (ArrayNode) resp.get("items");
                    if (items == null || items.isEmpty()) { done = true; return; }
                    List<JsonNode> list = new ArrayList<>();
                    items.forEach(list::add);
                    current = list.iterator();
                    if (items.size() < count) done = true; else offset += count;
                } catch (VkApiClient.VkApiException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    static Iterable<JsonNode> iteratePostComments(VkApiClient client, int ownerId, int postId) throws VkApiClient.VkApiException {
        return () -> new Iterator<JsonNode>() {
            int offset = 0;
            final int count = 100;
            Iterator<JsonNode> current = Collections.emptyIterator();
            boolean done = false;

            @Override public boolean hasNext() {
                fetchIfNeeded();
                return current.hasNext();
            }
            @Override public JsonNode next() {
                fetchIfNeeded();
                return current.next();
            }
            private void fetchIfNeeded() {
                if (done || current.hasNext()) return;
                try {
                    JsonNode resp = client.call("wall.getComments", Map.of(
                            "owner_id", ownerId,
                            "post_id", postId,
                            "count", count,
                            "offset", offset,
                            "sort", "asc",
                            "thread_items_count", 10,
                            "extended", 0
                    ));
                    ArrayNode items = (ArrayNode) resp.get("items");
                    if (items == null || items.isEmpty()) { done = true; return; }
                    List<JsonNode> list = new ArrayList<>();
                    items.forEach(list::add);
                    current = list.iterator();
                    LOG.debug("wall.getComments returned {} items at offset {} (owner_id={} post_id={})", items.size(), offset, ownerId, postId);
                    if (items.size() < count) done = true; else offset += count;
                } catch (VkApiClient.VkApiException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    //! Метод для WatchMode - отдельная кафка
    static void fetchCommentsParallelWithKafka(VkApiClient client, int ownerId, List<Map<String, Object>> posts, int parallel, 
                                               KafkaProducer<String, byte[]> producer, String kafkaTopic) throws Exception {
        var pool = Executors.newFixedThreadPool(Math.max(1, parallel));
        List<Future<Integer>> futures = new ArrayList<>();
        for (Map<String, Object> p : posts) {
            int postId = ((Number) p.get("id")).intValue();
            Map<?, ?> commentsMap = (Map<?, ?>) p.get("comments");
            int commentsCount = 0;
            if (commentsMap != null) {
                Object cc = commentsMap.get("count");
                if (cc instanceof Number) {
                    commentsCount = ((Number) cc).intValue();
                }
            }
            if (commentsCount <= 0) {
                LOG.debug("Post {} has no comments. Skip.", postId);
                continue;
            }
            final int ccFinal = commentsCount;
            LOG.info("Fetching comments for post {} ({} total)", postId, ccFinal);
            futures.add(pool.submit(() -> fetchCommentsForPostWithKafka(client, ownerId, postId, producer, kafkaTopic)));
        }
        
        int totalSent = 0;
        for (Future<Integer> f : futures) {
            try { 
                totalSent += f.get(); 
            } catch (Exception e) { 
                LOG.error("Error fetching comments in parallel: {}", e.toString()); 
            }
        }
        pool.shutdown();
        
        if (producer != null) {
            producer.flush();
            LOG.info("Sent {} total comments to Kafka topic '{}'", totalSent, kafkaTopic);
        }
    }
    //! Метод для WatchMode - отдельная кафка
    static void fetchCommentsForPostWithPartitionedKafka(
                                                        VkApiClient client,
                                                        int ownerId,
                                                        int postId,
                                                        KafkaProducer<String, byte[]> producer,
                                                        String kafkaTopic
                                                        ) {
        int totalFetched = 0;
        int filtered = 0;
        int sentToKafka = 0;
        
        try {
            for (JsonNode c : iteratePostComments(client, ownerId, postId)) {
                totalFetched += 1;
                if (isValidComment(c)) {
                    Map<String, Object> m = JsonUtil.toMap(c);
                    m.put("_owner_id", ownerId);
                    m.put("_post_id", postId);
                    
                    if (producer != null && kafkaTopic != null) {
                        try {
                            String key = ownerId + "_" + postId + "_" + m.get("id");
                            // ИСПОЛЬЗУЕМ НОВЫЙ МЕТОД С ПАРТИЦИОНИРОВАНИЕМ
                            KafkaHelper.sendJsonToPartition(producer, kafkaTopic, ownerId, key, m);
                            sentToKafka++;
                        } catch (Exception e) {
                            LOG.error("Failed to send comment to Kafka: {}", e.toString());
                        }
                    }
                } else {
                    filtered += 1;
                }
            }
        } catch (Exception e) {
            LOG.error("Error fetching comments for Kafka for post {}: {}", postId, e.getMessage());
        }
        
        if (filtered > 0 || sentToKafka > 0) {
            LOG.info("Post {} (group {}): {} comments fetched, {} filtered, {} sent to Kafka partition {}", 
                     postId, ownerId, totalFetched, filtered, sentToKafka, Math.abs(ownerId));
        }
    }

    //! Метод для WatchMode - отдельная кафка 
    static int fetchCommentsForPostWithKafka(VkApiClient client, int ownerId, int postId, KafkaProducer<String, byte[]> producer, String kafkaTopic) throws VkApiClient.VkApiException {
        int totalFetched = 0;
        int filtered = 0;
        int sentToKafka = 0;
        
        for (JsonNode c : iteratePostComments(client, ownerId, postId)) {
            totalFetched += 1;
            if (isValidComment(c)) {
                Map<String, Object> m = JsonUtil.toMap(c);
                m.put("_owner_id", ownerId);
                m.put("_post_id", postId);
                
                // Отправляем в Kafka сразу после валидации
                if (producer != null && kafkaTopic != null) {
                    try {
                        String key = ownerId + "_" + postId + "_" + m.get("id");
                        KafkaHelper.sendJson(producer, kafkaTopic, key, m);
                        sentToKafka++;
                    } catch (Exception e) {
                        LOG.error("Failed to send comment to Kafka: {}", e.toString());
                    }
                }
            } else {
                filtered += 1;
            }
        }
        
        if (filtered > 0 || sentToKafka > 0) {
            LOG.info("Post {}: {} comments fetched, {} filtered, {} sent to Kafka", postId, totalFetched, filtered, sentToKafka);
        }
        
        return sentToKafka;
    }

    //! Метод для RunOnce
    static List<Map<String, Object>> fetchCommentsParallel(VkApiClient client, int ownerId, List<Map<String, Object>> posts, int parallel) throws Exception {
        var pool = Executors.newFixedThreadPool(Math.max(1, parallel));
        List<Future<List<Map<String, Object>>>> futures = new ArrayList<>();
        for (Map<String, Object> p : posts) {
            int postId = ((Number) p.get("id")).intValue();
            Map<?, ?> commentsMap = (Map<?, ?>) p.get("comments");
            int commentsCount = 0;
            if (commentsMap != null) {
                Object cc = commentsMap.get("count");
                if (cc instanceof Number) {
                    commentsCount = ((Number) cc).intValue();
                }
            }
            if (commentsCount <= 0) {
                LOG.debug("Post {} has no comments. Skip.", postId);
                continue;
            }
            final int ccFinal = commentsCount;
            LOG.info("Fetching comments for post {} ({} total)", postId, ccFinal);
            futures.add(pool.submit(() -> fetchAllCommentsForPost(client, ownerId, postId)));
        }
        List<Map<String, Object>> result = new ArrayList<>();
        for (Future<List<Map<String, Object>>> f : futures) {
            try { result.addAll(f.get()); } catch (Exception e) { LOG.error("Error fetching comments in parallel: {}", e.toString()); }
        }
        pool.shutdown();
        return result;
    }

    //! Метод для fetchCommentsParallel
    static List<Map<String, Object>> fetchAllCommentsForPost(VkApiClient client, int ownerId, int postId) throws VkApiClient.VkApiException {
        List<Map<String, Object>> comments = new ArrayList<>();
        int totalFetched = 0;
        int filtered = 0;
        for (JsonNode c : iteratePostComments(client, ownerId, postId)) {
            totalFetched += 1;
            if (isValidComment(c)) {
                Map<String, Object> m = JsonUtil.toMap(c);
                m.put("_owner_id", ownerId);
                m.put("_post_id", postId);
                comments.add(m);
            } else {
                filtered += 1;
            }
        }
        if (filtered > 0) {
            LOG.debug("Post {}: {} comments fetched, {} filtered, {} valid", postId, totalFetched, filtered, comments.size());
        }
        return comments;
    }


    //! Run and Watch mods
    static List<Integer> resolveOwnerIds(VkApiClient client, List<String> groups, boolean allMyGroups) throws VkApiClient.VkApiException {
        List<Integer> ownerIds = new ArrayList<>();
        if (allMyGroups) {
            JsonNode resp;
            int offset = 0; int count = 1000;
            while (true) {
                resp = client.call("groups.get", Map.of("count", count, "offset", offset));
                ArrayNode items = (ArrayNode) resp.get("items");
                if (items == null || items.isEmpty()) break;
                for (JsonNode id : items) ownerIds.add(-id.asInt());
                if (items.size() < count) break; else offset += count;
            }
        }
        if (groups != null) {
            for (String g : groups) {
                try { ownerIds.add(parseOwnerIdentifier(client, g)); } catch (VkApiClient.VkApiException e) { LOG.warn("Skip '{}': {}", g, e.getMessage()); }
            }
        }
        LinkedHashSet<Integer> uniq = new LinkedHashSet<>(ownerIds);
        return new ArrayList<>(uniq);
    }
    //! Метод для resolveOwnerIds, который используется для всех режимов
    static int parseOwnerIdentifier(VkApiClient client, String raw) throws VkApiClient.VkApiException {
        String s = raw.trim();
        if (s.matches("-?\\d+")) return Integer.parseInt(s);
        s = s.replace("https://", "").replace("http://", "");
        s = s.replace("vk.com/", "");
        if (s.startsWith("@")) s = s.substring(1);
        for (String prefix : List.of("club", "public", "event", "id")) {
            if (s.startsWith(prefix)) {
                String tail = s.substring(prefix.length());
                if (tail.matches("\\d+")) {
                    int objId = Integer.parseInt(tail);
                    return prefix.equals("id") ? objId : -objId;
                }
            }
        }
        JsonNode resp = client.call("utils.resolveScreenName", Map.of("screen_name", s));
        String type = resp.path("type").asText("");
        int id = resp.path("object_id").asInt();
        if (type.equals("user")) return id;
        if (List.of("group", "page", "event").contains(type)) return -id;
        throw new VkApiClient.VkApiException(400, "Unsupported VK object type: " + type + " for " + raw);
    }

    //! Метод для RunOnce
    static LocalDate parseDate(String date) {
        if (date == null || date.isBlank()) return null;
        return LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }


    //! Метод для WatchMode
    @SuppressWarnings("unchecked")
    static Map<String, Object> getMap(Map<String, Object> parent, String key) {
        Object existing = parent.get(key);
        if (existing instanceof Map<?, ?> m) {
            try {
                return (Map<String, Object>) m;
            } catch (ClassCastException e) {
                // fall-through to replace with new map
            }
        }
        Map<String, Object> created = new LinkedHashMap<>();
        parent.put(key, created);
        return created;
    }
}
