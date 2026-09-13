import org.slf4j.LoggerFactory;

public class LoggingConfig {
    public static void configure(String level) {
        try {
            ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) 
                LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
            ch.qos.logback.classic.Level target = switch (String.valueOf(level).toUpperCase()) {
                case "TRACE" -> ch.qos.logback.classic.Level.TRACE;
                case "DEBUG" -> ch.qos.logback.classic.Level.DEBUG;
                case "WARN" -> ch.qos.logback.classic.Level.WARN;
                case "ERROR" -> ch.qos.logback.classic.Level.ERROR;
                default -> ch.qos.logback.classic.Level.INFO;
            };
            root.setLevel(target);
        } catch (Throwable t) {
            // If logback not present, ignore
        }
    }
}