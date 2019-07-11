package in.nimbo;

import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

/**
 * Unit test for RSS-feed app.
 */
public class AppTest {

    private static Connection conn;
    private static String url;
    private static String name;
    private static String user;
    private static String password;


    @BeforeClass
    public static void init() throws Exception {
        String JDBC_DRIVER = "org.h2.Driver";
        Class.forName(JDBC_DRIVER);
        ExternalData props = new ExternalData("src/test/resources/data.properties");
        url = props.getPropertyValue("url");
        name = props.getPropertyValue("table1");
        user = props.getPropertyValue("user");
        password = props.getPropertyValue("password");
        conn = DriverManager.getConnection(url, user, password);
        conn.prepareStatement("create table " + name + " (id int, name text, date timestamp (6) without time zone);")
                .executeUpdate();
        PreparedStatement preparedStatement = conn.prepareStatement("insert into " + name + " values(1, 'nimbo', ?);" +
                "insert into " + name + " values (2, 'sahab', ?);" +
                "insert into " + name + " values (3, 'sahab', ?);" +
                "insert into " + name + " values (4, 'sahab', ?);" +
                "insert into " + name + " values (5, 'sahab', ?);" +
                "insert into " + name + " values (6, 'sahab', ?);");
        preparedStatement.setTimestamp(1, new Timestamp(20000));
        preparedStatement.setTimestamp(2, new Timestamp(20000));
        preparedStatement.setTimestamp(3, new Timestamp(20000));
        preparedStatement.setTimestamp(4, new Timestamp(20000));
        preparedStatement.setTimestamp(5, new Timestamp(20000));
        preparedStatement.setTimestamp(6, new Timestamp(20000));
        preparedStatement.executeUpdate();
    }

    @Test
    public void addNewRss() {

    }

    /**
     * testing empty resultSet
     */
    @Test
    public void resultSetSize1() throws Exception {
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM " + name + " where id = -1;"
                , ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet resultSet = preparedStatement.executeQuery();
        int len = App.resultSetSize(resultSet);
        Assert.assertEquals(len, 0);
    }

    /**
     * testing resultSet with size of 1
     */
    @Test
    public void resultSetSize2() throws Exception {
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM " + name + " where id = 1;"
                , ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet resultSet = preparedStatement.executeQuery();
        int len = App.resultSetSize(resultSet);
        Assert.assertEquals(len, 1);
    }

    /**
     * testing resultSet with size of 5
     */
    @Test
    public void resultSetSize3() throws Exception{
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM " + name + " where name = 'sahab';"
                , ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet resultSet = preparedStatement.executeQuery();
        int len = App.resultSetSize(resultSet);
        Assert.assertEquals(len, 5);
    }

    @AfterClass
    public static void droppingAll() throws Exception{
        if (conn.isClosed())
            conn = DriverManager.getConnection(url, user, password);
        PreparedStatement preparedStatement = conn.prepareStatement("drop table " + name + ";");
        preparedStatement.execute();
        preparedStatement.close();
        conn.close();
    }
}
