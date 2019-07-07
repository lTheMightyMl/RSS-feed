package in.nimbo;

import in.nimbo.exception.BadPropertiesFile;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ExternalDataTest {

    /**
     * Only in case of catching exception this method is wrong
     */
    @Test
    public void loadProperties() {
        try {
            ExternalData.loadProperties("src/test/resources/externalDatas.properties");
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
            ExternalData.loadProperties("src/test/resources/badExternalDatas.properties");
        } catch (BadPropertiesFile | IOException badPropertiesFile) {
            assertTrue(true);
            return;
        }
        fail();
    }

    @Test
    public void badPathLoadProperties() {
        try {
            ExternalData.loadProperties("/some/wrong/address/data.properties");
        } catch (IOException e) {
            assertTrue(true);
        } catch (BadPropertiesFile badPropertiesFile) {
            fail();
        }
        fail();
    }

    @Test
    public void getPropertyValueTest1() {
        try {
            ExternalData.loadProperties("src/test/resources/externalDatas.properties");
        } catch (IOException | BadPropertiesFile e) {
            fail();
        }
        assertEquals(ExternalData.getPropertyValue("user"), "var");
    }

    @Test
    public void getPropertyValueTest2() {
        try {
            ExternalData.loadProperties("src/test/resources/externalDatas.properties");
        } catch (IOException | BadPropertiesFile e) {
            fail();
        }
        assertEquals(ExternalData.getPropertyValue("user"), "var");
    }

    @Test
    public void addProperty() {
    }

    @Test
    public void getAllAgencies() {
    }
}