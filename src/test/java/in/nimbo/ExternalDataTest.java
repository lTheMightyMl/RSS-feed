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
//            badPropertiesFile.printStackTrace();
            fail();
        }
    }

    /**
     * it must give a exception to pass the test
     */
    @Test
    public void badLoadProperties() {
        try {
            ExternalData.loadProperties("src/test/resources/badExternalDatas.properties");
        } catch (BadPropertiesFile | IOException badPropertiesFile) {
//            badPropertiesFile.printStackTrace();
            assertTrue(true);
        }
        fail();
    }

    @Test
    public void getPropertyValue() {
        try {
            ExternalData.loadProperties("src/test/resources/externalDatas.properties");
        } catch (BadPropertiesFile | IOException badPropertiesFile) {
//            badPropertiesFile.printStackTrace();
        }
    }

    @Test
    public void addProperty() {
    }

    @Test
    public void getAllAgencies() {
    }
}