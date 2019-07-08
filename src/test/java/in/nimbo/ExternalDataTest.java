package in.nimbo;

import in.nimbo.exception.BadPropertiesFile;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class ExternalDataTest {

    /**
     * Only in case of catching exception this method is wrong
     */
    @Test
    public void loadProperties() {
        try {
            ExternalData probs = new ExternalData("src/test/resources/externalDatas.properties");
        } catch (BadPropertiesFile | IOException badPropertiesFile) {
            fail();
        }
        assertTrue(true);
    }

    /**
     * it must give a exception to pass the test
     * properties file don't contain db info's
     */
    @Test
    public void badLoadProperties() {
        try {
            ExternalData probs = new ExternalData("src/test/resources/badExternalDatas.properties");
        } catch (BadPropertiesFile | IOException badPropertiesFile) {
            assertTrue(true);
            return;
        }
        fail();
    }

    @Test
    public void badPathLoadProperties() {
        try {
            ExternalData probs = new ExternalData("/some/wrong/address/data.properties");
        } catch (IOException e) {
            assertTrue(true);
            return;
        } catch (BadPropertiesFile badPropertiesFile) {
            fail();
        }
        fail();
    }

    @Test
    public void getPropertyValueTest1() {
        ExternalData probs = null;
        try {
            probs = new ExternalData("src/test/resources/externalDatas.properties");
        } catch (IOException | BadPropertiesFile e) {
            fail();
        }
        assertEquals(probs.getPropertyValue("user"), "var2");
    }

    @Test
    public void getPropertyValueTest2() {
        ExternalData probs = null;
        try {
            probs = new ExternalData("src/test/resources/externalDatas.properties");
        } catch (IOException | BadPropertiesFile e) {
            fail();
        }
        assertEquals(probs.getPropertyValue("key3 test"), "value3.1 value3.2");
    }

    @Test
    public void addProperty() throws BadPropertiesFile, IOException {
        ExternalData probs = new ExternalData("src/test/resources/badExternalDatas.properties");
        probs.addProperty("test", "added");
        probs = new ExternalData("src/test/resources/badExternalDatas.properties");
        assertEquals(probs.getPropertyValue("test"), "added");
    }

    @Test
    public void getAllAgencies() throws BadPropertiesFile, IOException {
        ExternalData probs = new ExternalData("src/test/resources/externalDatas.properties");
        Set<String> keys = probs.getAllAgencies().keySet();
        Set<String> actualKeys = new HashSet<>();
        actualKeys.add("key1");
        actualKeys.add("key3 test");
        actualKeys.add("key4");
        for (String s : keys) {
            if (! actualKeys.contains(s)) {
                fail();
            }
        }

    }
}