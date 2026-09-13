import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import picocli.CommandLine;

public class ParserCLI implements Callable<Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(ParserCLI.class);

    @CommandLine.Option(names = "--token", required = false, defaultValue = "${env:VK_TOKEN}", description = "VK access token")
    String token;

    @CommandLine.Option(names = "--groups", split = " ")
    List<String> groups = new ArrayList<>();

    @CommandLine.Option(names = "--all-my-groups")
    boolean allMyGroups;

    @CommandLine.Option(names = "--out-dir", defaultValue = "vk_data")
    Path outDir;

    @CommandLine.Option(names = "--no-comments")
    boolean noComments = false;

    @CommandLine.Option(names = "--save-json")
    boolean saveJson = false;

    @CommandLine.Option(names = "--max-posts-per-group")
    Integer maxPostsPerGroup;

    @CommandLine.Option(names = "--since")
    String since;

    @CommandLine.Option(names = "--until")
    String until;

    @CommandLine.Option(names = "--parallel-comments", defaultValue = "${env:PARALLEL_COMMENTS:-1}")
    int parallelComments = 1;

    @CommandLine.Option(names = "--rps", defaultValue = "${env:VK_RPS:-1}")
    double rps = 3.0;

    @CommandLine.Option(names = "--http-pool", defaultValue = "${env:HTTP_POOL:-1}")
    int httpPool = 10;

    @CommandLine.Option(names = "--watch")
    boolean watch;

    @CommandLine.Option(names = "--poll-every", defaultValue = "${env:POLL_EVERY:-1}")
    int pollEvery = 30;

    @CommandLine.Option(names = "--state-file")
    Path stateFile;

    @CommandLine.Option(names = "--recent-posts-for-comments", defaultValue = "${env:RECENT_POSTS_FOR_COMMENTS:-1}")
    int recentPostsForComments = 50;

    @CommandLine.Option(names = "--log-level", defaultValue = "${env:LOG_LEVEL:-}")
    String logLevel = "INFO";

    private DatabaseService dbService;
    private Set<Long> processedGroups = new HashSet<>();

    @CommandLine.Option(names = "--kafka-bootstrap", defaultValue = "${env:KAFKA_BOOTSTRAP_SERVERS}", description = "Kafka bootstrap servers")
    String kafkaBootstrapServers;

    @CommandLine.Option(names = "--kafka-topic", defaultValue = "${env:KAFKA_TOPIC}", description = "Kafka topic for comments")
    String kafkaTopic;

    private KafkaProducer<String, byte[]> kafkaProducer;

    @Override
    public Integer call() throws Exception {
        if (logLevel != null && !logLevel.isEmpty()) {
            LoggingConfig.configure(logLevel);
        } else {
            LoggingConfig.configure(logLevel); // значение по умолчанию
        }
        if (token == null || token.isBlank()) {
            System.err.println("Provide VK access token via --token or VK_TOKEN env var");
            return 2;
        } 

        this.dbService = new DatabaseService();

        if (parallelComments <= 0) parallelComments = 1;
        if (rps <= 0.0) rps = 3.0;
        if (httpPool <= 0) httpPool = 10;
        if (pollEvery <= 0) pollEvery = 30;
        if (recentPostsForComments <= 0) recentPostsForComments = 50;

        VkApiClient client = new VkApiClient(token, rps, httpPool);
        try {
                LOG.info("Waiting 50 seconds for Kafka to start...");
                Thread.sleep(50000);
                this.kafkaProducer = KafkaHelper.createProducer(kafkaBootstrapServers);
                LOG.info("Kafka producer initialized for topic '{}' on '{}'", kafkaTopic, kafkaBootstrapServers);
        } catch (Exception e) {
            LOG.error("Failed to initialize Kafka producer: {}", e.getMessage());
            this.kafkaProducer = null;
        }

        if (watch) {
            WatchMode.run(
                            client,
                            groups,
                            allMyGroups,
                            outDir,
                            !noComments,
                            saveJson,
                            pollEvery,
                            recentPostsForComments,
                            parallelComments,
                            stateFile,
                            dbService,
                            kafkaProducer,
                            kafkaTopic
                        );
        } else {
            RunOnceMode.run(
                            client,
                            groups,
                            allMyGroups,
                            outDir,
                            !noComments,
                            maxPostsPerGroup,
                            since,
                            until,
                            parallelComments,
                            dbService
                        );
        }
        return 0;
    }

}


