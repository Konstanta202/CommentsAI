import java.nio.file.Path;
import java.util.List;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VKParserService {

    private static final Logger LOG = LoggerFactory.getLogger(VKParserService.class);
    
    private final String token;
    private final List<String> groups;
    private final boolean allMyGroups;
    private final Path outDir;
    private final boolean includeComments;
    private final boolean saveJson;
    private final Integer maxPostsPerGroup;
    private final String since;
    private final String until;
    private final int parallelComments;
    private final double rps;
    private final int httpPool;
    private final int pollEvery;
    private final Path stateFile;
    private final int recentPostsForComments;
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private KafkaProducer<String, byte[]> kafkaProducer;
    
    public VKParserService(String token, List<String> groups, boolean allMyGroups, Path outDir, 
                            boolean includeComments, boolean saveJson, Integer maxPostsPerGroup, 
                            String since, String until, int parallelComments, double rps, 
                            int httpPool, int pollEvery, Path stateFile, int recentPostsForComments,
                            String kafkaBootstrapServers, String kafkaTopic) {
        this.token = token;
        this.groups = groups;
        this.allMyGroups = allMyGroups;
        this.outDir = outDir;
        this.includeComments = includeComments;
        this.saveJson = saveJson;
        this.maxPostsPerGroup = maxPostsPerGroup;
        this.since = since;
        this.until = until;
        this.parallelComments = parallelComments;
        this.rps = rps;
        this.httpPool = httpPool;
        this.pollEvery = pollEvery;
        this.stateFile = stateFile;
        this.recentPostsForComments = recentPostsForComments;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaTopic = kafkaTopic;
    }

    public void runOnceMode() throws Exception {
        DatabaseService dbService = new DatabaseService();
        VkApiClient client = new VkApiClient(token, rps, httpPool);
        // FileService fileService = new FileService();
        try {
                LOG.info("Waiting for Kafka initialization...");
                Thread.sleep(50000);
                this.kafkaProducer = KafkaHelper.createProducer(kafkaBootstrapServers);

                // RunOnceMode runOnce = new RunOnceMode(client, dbService, kafkaProducer);
                RunOnceMode.run(
                                client,
                                groups,
                                allMyGroups,
                                outDir,
                                includeComments,
                                maxPostsPerGroup,
                                since,
                                until,
                                parallelComments,
                                dbService
                            );
        } finally {
            kafkaProducer.close();
        }
    }

    public void runWatchMode() throws Exception {
        DatabaseService dbService = new DatabaseService();
        VkApiClient client = new VkApiClient(token, rps, httpPool);
        // KafkaService kafkaService = new KafkaService(kafkaBootstrapServers, kafkaTopic);
        // FileService fileService = new FileService();
        
        try {
            // Ждем инициализацию Kafka
                LOG.info("Waiting for Kafka initialization...");
                Thread.sleep(50000);
                this.kafkaProducer = KafkaHelper.createProducer(kafkaBootstrapServers);
                WatchMode.run(
                                client,
                                groups,
                                allMyGroups,
                                outDir,
                                includeComments,
                                saveJson,
                                pollEvery,
                                recentPostsForComments,
                                parallelComments,
                                stateFile,
                                dbService,
                                kafkaProducer,
                                kafkaTopic
                            );
            } finally {
                kafkaProducer.close();
            }
    }
}