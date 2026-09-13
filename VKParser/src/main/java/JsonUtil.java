import com.fasterxml.jackson.databind.JsonNode;

import java.util.*;

public class JsonUtil {
    public static Map<String, Object> toMap(JsonNode node) {
        Map<String, Object> map = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            map.put(e.getKey(), toJava(e.getValue()));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static Object toJava(JsonNode node) {
        if (node.isObject()) {
            return toMap(node);
        } else if (node.isArray()) {
            List<Object> list = new ArrayList<>();
            node.forEach(n -> list.add(toJava(n)));
            return list;
        } else if (node.isTextual()) {
            return node.asText();
        } else if (node.isNumber()) {
            if (node.isIntegralNumber()) return node.asLong();
            return node.asDouble();
        } else if (node.isBoolean()) {
            return node.asBoolean();
        } else if (node.isNull()) {
            return null;
        }
        return node.asText();
    }
}


