package in.nimbo.database;

import in.nimbo.ExternalData;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

public class TableTest {

    private static Table table;
    private static Connection conn;
    private static String url;
    private static String user;
    private static String password;
    private static String name;

    @BeforeClass
    public static void creatingTable() throws Exception {
        String JDBC_DRIVER = "org.h2.Driver";
        Class.forName(JDBC_DRIVER);
        ExternalData props = new ExternalData("src/test/resources/data.properties");
        url = props.getPropertyValue("url");
        name = props.getPropertyValue("table");
        user = props.getPropertyValue("user");
        password = props.getPropertyValue("password");
        table = new Table(name, props);
        conn = DriverManager.getConnection(url, user, password);

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM " + name + ";");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getMetaData());
        }
    }

    @Test
    public void creationTest() throws Exception {
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM " + name + ";");
        preparedStatement.executeQuery();
        Assert.assertNotNull(preparedStatement);
    }

    @Test
    public void insert() throws SQLException {
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        table.insert("agency", "a new news", new Date(100000), "this is a new new for testing"
                    , "ali");
        PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM " + name + " WHERE agency = 'agency'" +
                " AND title = 'a new news' ;");
        ResultSet resultSet = preparedStatement.executeQuery();
        boolean r = resultSet.last();
        Assert.assertTrue(r);
    }

    @Test
    public void searchTitle() throws Exception{
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        table.insert("nimbo", "sahab internship", new Date(200000), "started :)"
                , "smska");
        ResultSet resultSet = table.searchTitle("sahab", 0, 10);
        boolean r = resultSet.last();
        Assert.assertTrue(r);
    }

    @Test
    public void searchTitleInDate() throws Exception{
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        table.insert("sahab", "sahab internship incoming", new Date(20000), "started ;)"
                , "smska");
        ResultSet resultSet = table.searchTitleInDate("sahab", new Date(10000) ,new Date(30000) ,0, 10);
        boolean r = resultSet.last();
        Assert.assertTrue(r);
    }

    @Test
    public void searchDescriptionInDate() throws Exception{
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        table.insert("sahab pardaz", "sahab internship incoming...", new Date(20000),
                "started :)) in the name of god :]", "aliam");
        ResultSet resultSet = table.searchDescriptionInDate("god", new Date(10000) ,new Date(30000),
                0, 10);
        boolean r = resultSet.last();
        Assert.assertTrue(r);
    }

    @Test
    public void searchOnTitleInSpecificSite() throws Exception{
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        table.insert("shahab", "shahab if from Amol", new Date(20000),
                "it seems than shahab is from Amol not Babol", "amir");
        ResultSet resultSet = table.searchOnTitleInSpecificSite("shahab","Amol" ,0, 10);
        boolean r = resultSet.last();
        Assert.assertTrue(r);
    }

    @Test
    public void searchOnContentInSpecificSite() throws Exception {
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        table.insert("yjc", "amir : shahab if from Amol", new Date(20000),
                "amir in shahab agency said that it seems than shahab is from Amol not Babol", "reza");
        ResultSet resultSet = table.searchOnContentInSpecificSite("yjc","shahab" ,0, 10);
        boolean r = resultSet.last();
        Assert.assertTrue(r);
    }

    @Test
    public void searchOnContent() throws Exception{
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        table.insert("aee", "salam", new Date(20000),
                "it seems than shahab is from Amol not Babol, what a new :(", "Morteza");
        ResultSet resultSet = table.searchOnContent("Amol",0, 10);
        boolean r = resultSet.last();
        Assert.assertTrue(r);
    }

    @AfterClass
    public static void droppingAll() throws Exception{
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        PreparedStatement preparedStatement = conn.prepareStatement("drop table " + name + ";");
        preparedStatement.execute();
        preparedStatement.close();
        conn.close();
        table.close();
    }
}