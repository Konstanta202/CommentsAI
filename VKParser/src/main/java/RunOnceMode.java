import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class RunOnceMode {

    private static final Logger LOG = LoggerFactory.getLogger(RunOnceMode.class);
    
    // private static VkApiClient client;
    // private static DatabaseService dbService;
    // private static KafkaHelper kafkaService;
    // private static KafkaProducer<String, byte[]> kafkaProducer;

    // private static VKDataService vkDataService;


    static void run(
                    VkApiClient client,
                    List<String> groups,
                    boolean allMyGroups,
                    Path outDir,
                    boolean includeComments,
                    Integer maxPostsPerGroup,
                    String since,
                    String until,
                    int parallelComments,
                    DatabaseService dbService
                    ) throws Exception {
        List<Integer> ownerIds = VKDataService.resolveOwnerIds(client, groups, allMyGroups);
        
        if (ownerIds.isEmpty()) {
            LOG.error("No owners to process. Provide --groups or --all-my-groups.");
            return;
        }

        String timestamp = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(java.time.LocalDateTime.now());
        Path baseDir = outDir.resolve("vk_dump_" + timestamp);
        java.nio.file.Files.createDirectories(baseDir);

        LocalDate sinceDate = VKDataService.parseDate(since);
        LocalDate untilDate = VKDataService.parseDate(until);

        
        Map<String, Object> state = StateStore.load(baseDir);

        if (!state.containsKey("groups_info_collected")) {
            LOG.info("Collecting initial groups info for watch mode...");
            List<Map<String, Object>> groupsInfo = VKDataService.collectGroupsInfo(client, ownerIds, dbService);
            if (!groupsInfo.isEmpty()) {
                Path groupsInfoPath = baseDir.resolve("groups_info.jsonl");
                JsonlIO.write(groupsInfoPath, groupsInfo);
                LOG.info("Initial groups info saved to {} ({} groups)", groupsInfoPath, groupsInfo.size());
                
                // Проверяем, что файл действительно создался
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
            StateStore.save(baseDir, state);
        }

        for (int ownerId : ownerIds) {
            String ownerStr = ownerId < 0 ? ("group_" + (-ownerId)) : ("user_" + ownerId);
            LOG.info("Processing {} (owner_id={})…", ownerStr, ownerId);

            Path postsPath = baseDir.resolve(ownerStr + "_posts.jsonl");
            Path commentsPath = baseDir.resolve(ownerStr + "_comments.jsonl");

            List<Map<String, Object>> posts = new ArrayList<>();
            for (JsonNode post : VKDataService.iterateWallPosts(client, ownerId)) {
                long ts = post.path("date").asLong(0);
                LocalDate d = Instant.ofEpochSecond(ts).atZone(ZoneId.systemDefault()).toLocalDate();
                if (sinceDate != null && d.isBefore(sinceDate)) break;
                if (untilDate != null && d.isAfter(untilDate)) continue;

                VKDataService.savePostToDatabase(post, ownerId, dbService);

                posts.add(JsonUtil.toMap(post));
                if (maxPostsPerGroup != null && posts.size() >= maxPostsPerGroup) break;
            }
            JsonlIO.write(postsPath, posts);

            if (includeComments && !posts.isEmpty()) {

                VKDataService.fetchCommentsAndSaveToDatabase(client, ownerId, posts, parallelComments, dbService);

                List<Map<String, Object>> allComments = VKDataService.fetchCommentsParallel(client, ownerId, posts, parallelComments);
                JsonlIO.write(commentsPath, allComments);

                // String kafkaTopic = System.getenv("KAFKA_TOPIC");
                // String bootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
                // if (kafkaTopic != null && !kafkaTopic.isBlank() && bootstrap != null && !bootstrap.isBlank()) {
                //     try (KafkaProducer<String, byte[]> producer = KafkaHelper.createProducer(bootstrap)) {
                //         LOG.info("Kafka producer created for topic '{}' on '{}'", kafkaTopic, bootstrap);
                //         fetchCommentsParallelWithKafka(client, ownerId, posts, parallelComments, producer, kafkaTopic);
                //     } catch (Exception e) {
                //         LOG.error("Kafka unavailable; proceeding without producing: {}", e.toString());
                //     }
                // } else if (includeComments) {
                //     LOG.warn("Kafka not configured: set KAFKA_TOPIC and KAFKA_BOOTSTRAP_SERVERS to enable producing");
                //     // Fallback to old method without Kafka
                //     List<Map<String, Object>> allComments = fetchCommentsParallel(client, ownerId, posts, parallelComments);
                //     JsonlIO.write(commentsPath, allComments);
                // }
            }
            LOG.info("Finished {}", ownerStr);
        }
    }
}
