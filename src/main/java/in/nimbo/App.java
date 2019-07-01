package in.nimbo;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;

public class App {
    private static final String URL;
    private static final String USER;
    private static final String PASSWORD;
    private static final String TABLE;

    static {
        Properties databaseProperties = new Properties();
        try {
            databaseProperties.load(new FileInputStream(Thread.currentThread().getContextClassLoader().getResource(
                    "database.properties").getPath()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        URL = databaseProperties.getProperty("url");
        USER = databaseProperties.getProperty("user");
        PASSWORD = databaseProperties.getProperty("password");
        TABLE = databaseProperties.getProperty("table");
    }

    public static void main(String[] args) throws IOException, FeedException, SQLException {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            writeToDB(connection);
            readFromDB(connection);
        }
    }

    private static void writeToDB(Connection connection) throws IOException, FeedException, SQLException {
        Statement statement = connection.createStatement();
        statement.execute("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '" + TABLE + "') THEN CREATE TABLE " + TABLE + "(" +
                "    title          text,\n" +
                "    published_date text,\n" +
                "    description    text,\n" +
                "    author         text\n" +
                ");" +
                " END IF; END $$;");
        Properties newsAgencies = loadAgencies();
        Enumeration<?> agencyNames = newsAgencies.propertyNames();
        while (agencyNames.hasMoreElements()) {
            Object agency = agencyNames.nextElement();
            processAgency(connection, agency.toString(), newsAgencies.getProperty(agency.toString()));
        }
    }

    private static void processAgency(Connection connection, String agencyName, String agencyURL) throws FeedException,
            IOException {
        System.out.println(agencyName);
        SyndFeed feed = new SyndFeedInput().build(new XmlReader(new URL(agencyURL)));
        feed.getEntries().forEach(entry -> {
            processEntry(connection, entry);
        });
    }

    private static void processEntry(Connection connection, SyndEntry entry) {
        String title = entry.getTitle();
        Date publishedDate = entry.getPublishedDate();
        String description = entry.getDescription().getValue();
        String author = entry.getAuthor();
        System.out.println(title);
        System.out.println(publishedDate);
        System.out.println(description);
        System.out.println(author);
        System.out.println();
        try (Statement statement = connection.createStatement()) {
            statement.closeOnCompletion();
            statement.executeUpdate("INSERT INTO " + TABLE + " VALUES ('" + title + "', '" + publishedDate + "', '"
                    + description + "', '" + author + "');");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Properties loadAgencies() throws IOException {
        Properties newsAgencies = new Properties();
        newsAgencies.load(new FileInputStream(Thread.currentThread().getContextClassLoader().getResource("news" +
                "Agencies.properties").getPath()));
        return newsAgencies;
    }

    private static void readFromDB(Connection connection) throws SQLException {
        try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM " + TABLE + ";")) {
            while (resultSet.next())
                System.err.println(resultSet.getString("title"));
        }
    }
}