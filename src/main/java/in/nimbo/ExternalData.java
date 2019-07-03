package in.nimbo;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

public class ExternalData {

    private static final String fileName = "externalDatas.properties";
    private static final String outPath = "src/main/resources/externalDatas.properties";
    private static Properties properties;
    private static HashSet<String> unvalidAgencyKey = new HashSet<>(Arrays.asList("url", "user", "password"));

    static void loadProperties() {
        properties = new Properties();
        try {
            properties.load(new FileInputStream(Objects.requireNonNull(Thread.currentThread().
                    getContextClassLoader().getResource(fileName)).getPath()));
        } catch (IOException e) {
//            LOGGER.error("", e);
            System.exit(0);
        }
    }

    public static String getPropertyValue(String key) {
        return properties.getProperty(key);
    }

    public static void addProperty(String key, String value) throws IOException {
        properties.setProperty(key, value);                 // add property
        properties.store(new FileOutputStream(outPath), null);  // save property in file, second parameter is ...
    }

    public static HashMap<String, String> getAllAgencies() {
        HashMap<String, String> agencies = new HashMap<>();
        String key;
        for (Map.Entry<Object, Object> property: properties.entrySet()) {
            key = (String) property.getKey();
            if (! unvalidAgencyKey.contains(key)) {
                agencies.put(key, (String) property.getValue());
            }
        }
        return agencies;
    }

}
