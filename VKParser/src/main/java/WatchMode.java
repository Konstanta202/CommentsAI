import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class WatchMode {
    private static final Logger LOG = LoggerFactory.getLogger(WatchMode.class);
    
    public static void run(
                    VkApiClient client,
                    List<String> groups,
                    boolean allMyGroups,
                    Path outDir,
                    boolean includeComments,
                    boolean saveJsonFile, 
                    int pollEverySec,
                    int recentPostsForComments,
                    int parallelComments,
                    Path customStateFile,
                    DatabaseService dbService,
                    KafkaProducer<String, byte[]> kafkaProducer,
                    String kafkaTopic
                    ) throws Exception {
        List<Integer> ownerIds = VKDataService.resolveOwnerIds(client, groups, allMyGroups);
        if (ownerIds.isEmpty()) {
            LOG.error("No owners to process. Provide --groups or --all-my-groups.");
            return;
        }

        Path baseDir = outDir.resolve("vk_watch");
        java.nio.file.Files.createDirectories(baseDir);
        Path statePath = (customStateFile != null) ? customStateFile : baseDir.resolve("watch_state.json");
        Map<String, Object> state = StateStore.load(statePath);

        // if (!state.containsKey("groups_info_collected")) {
            LOG.info("Collecting initial groups info for watch mode...");
            List<Map<String, Object>> groupsInfo = VKDataService.collectGroupsInfo(client, ownerIds, dbService);
            if (!groupsInfo.isEmpty()) {
                Path groupsInfoPath = baseDir.resolve("groups_info.jsonl");
                JsonlIO.write(groupsInfoPath, groupsInfo);
                LOG.info("Initial groups info saved to {} ({} groups)", groupsInfoPath, groupsInfo.size());
                if (java.nio.file.Files.exists(groupsInfoPath)) {
                    long fileSize = java.nio.file.Files.size(groupsInfoPath);
                    LOG.info("Groups info file created successfully. Size: {} bytes", fileSize);
                } else {
                    LOG.error("Groups info file was not created!");
                }
            } else {
                LOG.warn("No groups info collected for watch mode");
            }
            state.put("groups_info_collected", true);
            StateStore.save(statePath, state);
        // }

        // LOG.info("Collecting initial groups info for watch mode...");
        // List<Map<String, Object>> groupsInfo = collectGroupsInfo(client, ownerIds, dbService);

        // if (!groupsInfo.isEmpty()) {
        //     if (saveJsonFile) {
        //     Path groupsInfoPath = baseDir.resolve("groups_info.jsonl");
        //     JsonlIO.write(groupsInfoPath, groupsInfo);
        //     LOG.info("Initial groups info saved to {} ({} groups)", groupsInfoPath, groupsInfo.size());
        //     if (java.nio.file.Files.exists(groupsInfoPath)) {
        //         long fileSize = java.nio.file.Files.size(groupsInfoPath);
        //         LOG.info("Groups info file created successfully. Size: {} bytes", fileSize);
        //     } else {
        //         LOG.error("Groups info file was not created!");
        //     }
        //     }

        // } else {
        //     LOG.warn("No groups info collected for watch mode");
        // }
        // state.put("groups_info_collected", true);
        // StateStore.save(statePath, state);




        // Создаем пути для файлов комментариев
        Map<Integer, Path> commentsPaths = new LinkedHashMap<>();
        for (int ownerId : ownerIds) {
            String ownerStr = ownerId < 0 ? ("group_" + (-ownerId)) : ("user_" + ownerId);
            Path commentsPath = baseDir.resolve(ownerStr + "_comments.jsonl");
            commentsPaths.put(ownerId, commentsPath);
        }

        while (true) {
            long loopStart = System.currentTimeMillis();
            try {
                for (int ownerId : ownerIds) {
                    String ownerKey = String.valueOf(ownerId);
                    Map<String, Object> ownerState = VKDataService.getMap(state, ownerKey);
                    int lastSeenPostId = ((Number) ownerState.getOrDefault("last_post_id", 0)).intValue();

                    //! Вынести так, чтобы при флаге добавления в файл работало 
                    String ownerStr = ownerId < 0 ? ("group_" + (-ownerId)) : ("user_" + ownerId);
                    Path postsPath = baseDir.resolve(ownerStr + "_posts.jsonl");
                    Path commentsPath = commentsPaths.get(ownerId);

                    LOG.info("start get WALL");
                    //! , "extended": 1
                    JsonNode resp = client.call("wall.get", Map.of("owner_id", ownerId, "count", 100, "offset", 0));
                    ArrayNode items = (ArrayNode) resp.get("items");
                    List<JsonNode> newPosts = new ArrayList<>();
                    for (JsonNode p : items) {
                        int id = p.path("id").asInt(0);
                        if (id > lastSeenPostId) {
                            LOG.info("add new WALL");
                            newPosts.add(p);
                        }
                    }
                    if (!newPosts.isEmpty()) {
                        newPosts.sort(Comparator.comparingLong(n -> n.path("date").asLong(0)));
                        //! Сдела флаг для добавления в файл
                        List<Map<String, Object>> toWrite = new ArrayList<>();
                        for (JsonNode n : newPosts) toWrite.add(JsonUtil.toMap(n));
                        // для сохранения в БД
                        //!!! тут все ОК
                        for (JsonNode post : newPosts) {
                            LOG.info("start save POST to DATABASE");
                            VKDataService.savePostToDatabase(post, ownerId, dbService);
                        }
                        //! Сделать флаг 
                        JsonlIO.append(postsPath, toWrite);

                        lastSeenPostId = newPosts.stream().mapToInt(n -> n.path("id").asInt(0)).max().orElse(lastSeenPostId);
                        ownerState.put("last_post_id", lastSeenPostId);
                        LOG.info("{}: appended {} new posts", ownerStr, toWrite.size());
                    } else {
                        LOG.debug("{}: no new posts", ownerStr);
                    }

                    if (includeComments) {
                        for (JsonNode post : newPosts) {
                            int postId = post.path("id").asInt();
                            // fetchCommentsForPostWithPartitionedKafka(client, ownerId, postId, kafkaProducer, kafkaTopic);
                            //! В кафку идут айдишники коментов без всякого
                            VKDataService.fetchCommentsForPostWithKafka(client, ownerId, postId, kafkaProducer, kafkaTopic);
                            //! Тут начинается интерес
                            VKDataService.fetchCommentsForPostAndSaveToDatabase(client, ownerId, postId, dbService);
                        }

                        List<JsonNode> postsToCheck = new ArrayList<>();
                        for (int i = 0; i < Math.min(Math.max(1, recentPostsForComments), items.size()); i++) {
                            postsToCheck.add(items.get(i));
                        }
                        Map<String, Object> postsState = VKDataService.getMap(ownerState, "posts");

                        // Собирает все новые комментарии и возвращает из
                        var pool = Executors.newFixedThreadPool(Math.max(1, parallelComments));
                        List<Future<List<Map<String, Object>>>> futures = new ArrayList<>();
                        for (JsonNode post : postsToCheck) {
                            futures.add(pool.submit(() -> fetchNewCommentsForFile(client, ownerId, post, postsState)));
                        }
                        
                        // Собираем все комментарии для сохранения в файл
                        List<Map<String, Object>> allNewComments = new ArrayList<>();
                        int totalCommentsSavedToDB = 0;
                        for (Future<List<Map<String, Object>>> f : futures) {
                            try {
                                List<Map<String, Object>> comments = f.get();
                                allNewComments.addAll(comments);
                                
                                // Обновляем состояние для каждого поста
                                if (!comments.isEmpty()) {
                                    int maxCommentId = comments.stream()
                                        .mapToInt(c -> ((Number) c.get("id")).intValue())
                                        .max()
                                        .orElse(0);
                                    int postId = ((Number) comments.get(0).get("_post_id")).intValue();
                                    postsState.put(String.valueOf(postId), maxCommentId);
                                }
                            } catch (Exception e) {
                                LOG.error("{}: error checking comments: {}", ownerStr, e.toString());
                            }
                        }
                        pool.shutdown();
                        
                        if (!allNewComments.isEmpty()) {
                            LOG.info("Start add new comments to DB");
                            VKDataService.saveCommentsToDatabase(client, allNewComments, dbService, ownerId);
                            LOG.info("END");
                            LOG.info("Append new comment to KAFKA");
                            if (kafkaProducer != null) {
                                LOG.info("Sending {} new comment to Kafka", allNewComments.size());
                                for (Map<String, Object> comment: allNewComments) {
                                    String key = ownerId + "_" + comment.get("_post_id") + "_" + comment.get("id");
                                    KafkaHelper.sendJson(kafkaProducer, kafkaTopic, key, comment);
                                    // KafkaHelper.sendJsonToPartition(
                                    //                                 kafkaProducer,
                                    //                                 kafkaTopic,
                                    //                                 ownerId,
                                    //                                 key,
                                    //                                 comment
                                    //                                 );
                                }
                                LOG.info("{}: Sent {} new comments to Kafka", ownerStr, allNewComments.size());
                            }
                            JsonlIO.append(commentsPath, allNewComments);
                            LOG.info("{}: APPENDET {} new comments to file", ownerStr, allNewComments.size());
                        }
                        
                        ownerState.put("posts", postsState);
                    }
                }
                if (kafkaProducer != null) {
                    kafkaProducer.flush();
                    LOG.debug("Kafka producer flushed after processing all groups");
                }
    

                StateStore.save(statePath, state);
            } catch (Exception e) {
                LOG.error("Watch loop error: {}", e.toString());
            }

            long elapsed = System.currentTimeMillis() - loopStart;
            long sleepMs = Math.max(0, (pollEverySec * 1000L) - elapsed);
            if (sleepMs > 0) Thread.sleep(sleepMs);
        }
    }

    static List<Map<String, Object>> fetchNewCommentsForFile(VkApiClient client, int ownerId, JsonNode post, 
                                                            Map<String, Object> postsState) throws VkApiClient.VkApiException {
        int postId = post.path("id").asInt();
        int lastCid = ((Number) postsState.getOrDefault(String.valueOf(postId), 0)).intValue();
        List<Map<String, Object>> newComments = new ArrayList<>();
        int totalFetched = 0;
        int filtered = 0;

        for (JsonNode c : VKDataService.iteratePostComments(client, ownerId, postId)) {
            totalFetched += 1;
            if (VKDataService.isValidComment(c)) {
                int cid = c.path("id").asInt(0);
                
                // Добавляем только новые комментарии (cid > lastCid)
                if (cid > lastCid) {
                    Map<String, Object> commentMap = JsonUtil.toMap(c);
                    commentMap.put("_owner_id", ownerId);
                    commentMap.put("_post_id", postId);
                    newComments.add(commentMap);
                }
            } else {
                filtered += 1;
            }
        }
        
        if (totalFetched > 0) {
            LOG.debug("Post {}: {} comments fetched, {} filtered, {} new comments", 
                    postId, totalFetched, filtered, newComments.size());
        }
        
        return newComments;
    }
}
