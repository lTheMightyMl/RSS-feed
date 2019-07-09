package in.nimbo.database;

import in.nimbo.ExternalData;
import in.nimbo.exception.BadPropertiesFile;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;

public class TableTest {

    @Before
    public void creatingTable() throws BadPropertiesFile, IOException, SQLException {
        ExternalData props = new ExternalData("src/test/resources/data.properties");
        Table table = new Table("test", props);
        Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", "postgres", "");
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM test;");
        ResultSet resultSet = preparedStatement.executeQuery();
        boolean r = resultSet.last();   // Return false if the is no row.
        resultSet.close();
        connection.close();
        assert r;
    }

    @Test
    public void insert() throws BadPropertiesFile, IOException, SQLException {
        ExternalData props = new ExternalData("src/test/resources/data.properties");
        Table table = new Table("test", props);
        table.insert("agency", "a new new", new Date(100000), "this is a new new for testing"
                    , "ali");
        Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", "postgres", "");
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM test WHERE agency = agency;");
        ResultSet resultSet = preparedStatement.executeQuery();
        boolean r = resultSet.last();
        resultSet.close();
        connection.close();
        assert r;
    }

    @Test
    public void searchTitle() throws Exception{
        ExternalData props = new ExternalData("src/test/resources/data.properties");
        Table table = new Table("test", props);
        table.insert("nimbo", "sahab internship", new Date(200000), "started :)"
                , "smska");
        ResultSet resultSet = table.searchTitle("sahab", 0, 10);
        boolean r = resultSet.last();
        resultSet.close();
        assert r;
    }

    @Test
    public void searchTitleInDate() throws Exception{
        ExternalData props = new ExternalData("src/test/resources/data.properties");
        Table table = new Table("test", props);
        table.insert("sahab", "sahab internship incoming", new Date(20000), "started ;)"
                , "smska");
        ResultSet resultSet = table.searchTitleInDate("sahab", new Date(10000) ,new Date(30000) ,0, 10);
        boolean r = resultSet.last();
        resultSet.close();
        assert r;
    }

    @Test
    public void searchDescriptionInDate() throws Exception{
        ExternalData props = new ExternalData("src/test/resources/data.properties");
        Table table = new Table("test", props);
        table.insert("sahab pardaz", "sahab internship incoming...", new Date(20000),
                "started :)) in the name of god :]", "aliam");
        ResultSet resultSet = table.searchDescriptionInDate("god", new Date(10000) ,new Date(30000),
                0, 10);
        boolean r = resultSet.last();
        resultSet.close();
        assert r;
    }

    @Test
    public void searchOnTitleInSpecificSite() throws Exception{
        ExternalData props = new ExternalData("src/test/resources/data.properties");
        Table table = new Table("test", props);
        table.insert("shahab", "shahab if from Amol", new Date(20000),
                "it seems than shahab is from Amol not Babol", "amir");
        ResultSet resultSet = table.searchOnTitleInSpecificSite("shahab","Amol" ,0, 10);
        boolean r = resultSet.last();
        resultSet.close();
        assert r;
    }

    @Test
    public void searchOnContentInSpecificSite() throws Exception {
        ExternalData props = new ExternalData("src/test/resources/data.properties");
        Table table = new Table("test", props);
        table.insert("yjc", "amir : shahab if from Amol", new Date(20000),
                "amir in shahab agency said that it seems than shahab is from Amol not Babol", "reza");
        ResultSet resultSet = table.searchOnContentInSpecificSite("yjc","shahab" ,0, 10);
        boolean r = resultSet.last();
        resultSet.close();
        assert r;
    }

    @Test
    public void searchOnContent() throws Exception{
        ExternalData props = new ExternalData("src/test/resources/data.properties");
        Table table = new Table("test", props);
        table.insert("aee", "salam", new Date(20000),
                "it seems than shahab is from Amol not Babol, what a new :(", "Morteza");
        ResultSet resultSet = table.searchOnContent("Amol",0, 10);
        boolean r = resultSet.last();
        resultSet.close();
        assert r;
    }
}