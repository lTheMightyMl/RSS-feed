package in.nimbo;

import in.nimbo.exception.BadPropertiesFile;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

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
        assertEquals(probs.getPropertyValue("user"), "var");
    }

    @Test
    public void getPropertyValueTest2() {
        ExternalData probs = null;
        try {
            probs = new ExternalData("src/test/resources/externalDatas.properties");
        } catch (IOException | BadPropertiesFile e) {
            fail();
        }
        assertEquals(probs.getPropertyValue("user"), "var");
    }

    @Test
    public void addProperty() {
    }

    @Test
    public void getAllAgencies() {
    }
}