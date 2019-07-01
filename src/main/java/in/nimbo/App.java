package in.nimbo;

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
    private static final String NEWS_AGENCIES = "NEWS_AGENCIES";

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
        Properties newsAgencies = new Properties();
        newsAgencies.load(new FileInputStream(Thread.currentThread().getContextClassLoader().getResource("news" +
                "Agencies.properties").getPath()));
        Enumeration<?> propertyNames = newsAgencies.propertyNames();
        Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
        while (propertyNames.hasMoreElements()) {
            String agencyName = propertyNames.nextElement().toString();
            URL feedSource = new URL(newsAgencies.getProperty(agencyName));
            SyndFeedInput input = new SyndFeedInput();
            SyndFeed feed = input.build(new XmlReader(feedSource));
            System.out.println(agencyName);
            feed.getEntries().forEach(entry -> {
                String title = entry.getTitle();
                Date publishedDate = entry.getPublishedDate();
                String description = entry.getDescription().getValue();
                String author = entry.getAuthor();
                System.out.println(title);
                System.out.println(publishedDate);
                System.out.println(description);
                System.out.println(author);
                try {
                    Statement statement = connection.createStatement();
                    statement.closeOnCompletion();
                    statement.executeUpdate("INSERT INTO " + TABLE + " VALUES ('" + title + "', '" + publishedDate + "', '" + description + "', '" + author + "');");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }
        ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM " + TABLE + ";");
        while (resultSet.next()) {
            System.err.println(resultSet.getString("title"));
        }
    }
}