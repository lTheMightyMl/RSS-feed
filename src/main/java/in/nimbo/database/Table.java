package in.nimbo.database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;

public class Table {
    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);
    private static final String URL;
    private static final String USER;
    private static final String PASSWORD;
    private String name;

    static {
        Properties databaseProperties = new Properties();
        try {
            databaseProperties.load(new FileInputStream(Objects.requireNonNull(Thread.currentThread().
                    getContextClassLoader().getResource("database.properties")).getPath()));
        } catch (IOException e) {
            LOGGER.error("", e);
        }
        URL = databaseProperties.getProperty("url");
        USER = databaseProperties.getProperty("user");
        PASSWORD = databaseProperties.getProperty("password");
    }

    public Table(String name) throws SQLException {
        try (final Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             final PreparedStatement preparedStatement = connection.prepareStatement(String.format("DO $$ BEGIN IF " +
                     "NOT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name" +
                     " = '%s') THEN CREATE TABLE %s(agency text, title text, published_date timestamp without time " +
                     "zone, description text, author text); END IF; END $$;", name, name))) {
            // its better to use execute() https://jdbc.postgresql.org/documentation/head/ddl.html
            preparedStatement.executeUpdate();
        }
        this.name = name;
    }

    public void insert(String agencyName, String title, Date publishedDate, String description, String author) throws SQLException {
        try (final Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             final PreparedStatement preparedStatement = connection.prepareStatement(String.format("INSERT INTO %s " +
                     "VALUES ('%s', '%s', TIMESTAMP (6) WITHOUT TIME ZONE '%s', ' %s', '%s')", name, agencyName, title, new
                     Timestamp(publishedDate.getTime()), description, author))) {
            preparedStatement.executeUpdate();
        }
    }
  
    public ResultSet searchOnTitleInSpecificSite(String agencyName, String title) throws SQLException {

        try (final Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             final PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " +
                     "? WHERE agency = ? AND title LIKE '%?%';")) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, agencyName);
            preparedStatement.setString(3, title);
            return preparedStatement.executeQuery();
        }
    }

    public ResultSet searchOnContentInSpecificSite(String agencyName, String content) throws SQLException {
        try (final Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             final PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " +
                     "? WHERE agency = ? AND description LIKE '%?%'")) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, agencyName);
            preparedStatement.setString(3, content);
            return preparedStatement.executeQuery();
        }
    }

    public ResultSet searchOnContent(String content) throws SQLException {
        try (final Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             final PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " +
                     "? WHERE description LIKE '%?%'")) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, content);
            return preparedStatement.executeQuery();
        }
    }
}
