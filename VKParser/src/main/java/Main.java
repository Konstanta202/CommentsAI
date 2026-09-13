import picocli.CommandLine;


public class Main {
    public static void main(String[] args) {
        int code = new CommandLine(new ParserCLI()).execute(args);
        System.exit(code);
    }
}


