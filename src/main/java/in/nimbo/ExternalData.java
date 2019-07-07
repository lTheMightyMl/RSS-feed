package in.nimbo;

import in.nimbo.exception.BadPropertiesFile;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import static in.nimbo.App.LOGGER;

public class ExternalData {

    private static String propertiesPath;
    private static Properties properties;
    private static HashSet<String> reservedKeysForDB = new HashSet<>(Arrays.asList("db.url", "db.user", "db.password", "db.table"));

    static void loadProperties(String path) throws BadPropertiesFile, IOException {
        propertiesPath = path;

        properties = new Properties();
        properties.load(new FileInputStream(propertiesPath));

        checkValid();
    }

    private static void checkValid() throws BadPropertiesFile {

        int count = 0 ;
        for(String reservedKey : reservedKeysForDB)
            if(properties.containsKey(reservedKey))
                ++ count;

        if (count != reservedKeysForDB.size()) {
            throw new BadPropertiesFile("database properties missing in properties file");
        }
    }

    public static String getPropertyValue(String key) {
        return properties.getProperty("db." + key) == null ? properties.getProperty("agencies." + key) : properties.getProperty("db." + key);
    }

    public static void addProperty(String key, String value) throws IOException {

        // Checking that the property name is valid
        if (reservedKeysForDB.contains("db." + key)) {
            LOGGER.error("invalid property name");
            return;
        }

        // add property
        properties.setProperty("agencies." + key, value);

        // Store will save new property, second parameter is comment that will show up in the first line of file
        properties.store(new FileOutputStream(propertiesPath), null);
    }

    static HashMap<String, String> getAllAgencies() {
        HashMap<String, String> agencies = new HashMap<>();
        String key;
        for (Map.Entry<Object, Object> property: properties.entrySet()) {
            key = ((String) property.getKey());
            if (! reservedKeysForDB.contains(key)) {
                key = key.substring(9);
                agencies.put(key, (String) property.getValue());
            }
        }
        return agencies;
    }

}
