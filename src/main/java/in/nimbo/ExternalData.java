package in.nimbo;

import in.nimbo.exception.BadPropertiesFile;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

public class ExternalData {

    private static String propertiesPath;
    private static Properties properties;
    private static HashSet<String> reservedKeysForDB = new HashSet<>(Arrays.asList("url", "user", "password"));

    static void loadProperties(String path) throws BadPropertiesFile, IOException {
        propertiesPath = path;

        properties = new Properties();
        properties.load(new FileInputStream(propertiesPath));

        checkValid();
    }

    private static void checkValid() throws BadPropertiesFile {
        int[] flag = {0};

        reservedKeysForDB.forEach(s -> {
            if (properties.containsKey(s)) {
                ++ flag[0];
            }
        });

        if (flag[0] != reservedKeysForDB.size()) {
            throw new BadPropertiesFile("database properties missing in properties file");
        }
    }

    public static String getPropertyValue(String key) {
        return properties.getProperty(key);
    }

    public static void addProperty(String key, String value) throws IOException {

        // Checking that the property name is valid
        if (reservedKeysForDB.contains(key)) {
//            LOGGER.error("")
            return;
        }

        // add property
        properties.setProperty(key, value);

        // Store will save new property, second parameter is comment that will show up in the first line of file
        properties.store(new FileOutputStream(propertiesPath), null);
    }

    public static HashMap<String, String> getAllAgencies() {
        HashMap<String, String> agencies = new HashMap<>();
        String key;
        for (Map.Entry<Object, Object> property: properties.entrySet()) {
            key = (String) property.getKey();
            if (! reservedKeysForDB.contains(key)) {
                agencies.put(key, (String) property.getValue());
            }
        }
        return agencies;
    }

}
