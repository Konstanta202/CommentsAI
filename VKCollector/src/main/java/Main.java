import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import picocli.CommandLine;

/**
 * Разовый сбор постов и комментариев из пабликов VK в JSONL-файлы.
 * Без Kafka и баз данных: только VK API → папка с файлами.
 */
@CommandLine.Command(name = "vk-collector", mixinStandardHelpOptions = true,
        description = "Собирает посты и комментарии из пабликов VK в JSONL-файлы")
public class Main implements Callable<Integer> {

    @CommandLine.Option(names = "--token", description = "Ключ доступа VK (по умолчанию VK_TOKEN из окружения или .env)")
    String token;

    @CommandLine.Option(names = "--groups", split = ",", required = true,
            description = "Паблики через запятую: screen_name, ссылка, club123 или числовой id")
    List<String> groups = new ArrayList<>();

    @CommandLine.Option(names = "--out-dir", defaultValue = "data", description = "Куда сохранять (по умолчанию: ${DEFAULT-VALUE})")
    Path outDir;

    @CommandLine.Option(names = "--since", description = "Собирать посты не раньше даты, yyyy-MM-dd")
    LocalDate since;

    @CommandLine.Option(names = "--until", description = "Собирать посты не позже даты, yyyy-MM-dd")
    LocalDate until;

    @CommandLine.Option(names = "--max-posts", description = "Максимум постов на один паблик")
    Integer maxPosts;

    @CommandLine.Option(names = "--no-comments", description = "Не собирать комментарии")
    boolean noComments;

    @CommandLine.Option(names = "--rps", defaultValue = "3", description = "Запросов к VK API в секунду (по умолчанию: ${DEFAULT-VALUE})")
    double rps;

    @Override
    public Integer call() throws Exception {
        if (token == null || token.isBlank()) token = System.getenv("VK_TOKEN");
        if (token == null || token.isBlank()) token = readTokenFromDotEnv(Path.of(".env"));
        if (token == null || token.isBlank()) {
            System.err.println("Не найден ключ VK: передайте --token, задайте VK_TOKEN или положите его в .env");
            return 2;
        }

        VkApiClient client = new VkApiClient(token, rps, 1);
        Collector collector = new Collector(client, outDir, since, until, maxPosts, !noComments);
        collector.run(groups);
        return 0;
    }

    private static String readTokenFromDotEnv(Path file) throws IOException {
        if (!Files.exists(file)) return null;
        for (String line : Files.readAllLines(file)) {
            line = line.trim();
            if (line.startsWith("VK_TOKEN=")) return line.substring("VK_TOKEN=".length()).trim();
        }
        return null;
    }

    public static void main(String[] args) {
        CommandLine cli = new CommandLine(new Main());
        cli.registerConverter(LocalDate.class, LocalDate::parse);
        System.exit(cli.execute(args));
    }
}
