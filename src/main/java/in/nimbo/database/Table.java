package in.nimbo.database;

import in.nimbo.ExternalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Date;

public class Table {
    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);
    private static final String URL = ExternalData.getPropertyValue("url");
    private static final String USER = ExternalData.getPropertyValue("user");
    private static final String PASSWORD = ExternalData.getPropertyValue("password");
    private final Connection searchTitleConnection;
    private final Connection searchTitleInDateConnection;
    private final Connection searchDescriptionInDateConnection;
    private final Connection searchOnTitleInSpecificSiteConnection;
    private final Connection searchOnContentInSpecificSiteConnection;
    ;
    private final Connection searchOnContentConnection;
    private final PreparedStatement searchTitle;
    private final PreparedStatement searchTitleInDate;
    private final PreparedStatement searchDescriptionInDate;
    private final PreparedStatement searchOnTitleInSpecificSite;
    private final PreparedStatement searchOnContentInSpecificSite;
    private final PreparedStatement searchOnContent;
    private String name;

    public Table(final String name) throws SQLException {
        searchTitleConnection = DriverManager.getConnection(URL, USER, PASSWORD);
        searchTitleInDateConnection = DriverManager.getConnection(URL, USER, PASSWORD);
        searchDescriptionInDateConnection = DriverManager.getConnection(URL, USER, PASSWORD);
        searchOnTitleInSpecificSiteConnection = DriverManager.getConnection(URL, USER, PASSWORD);
        searchOnContentInSpecificSiteConnection = DriverManager.getConnection(URL, USER, PASSWORD);
        searchOnContentConnection = DriverManager.getConnection(URL, USER, PASSWORD);
        try (final Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             final Statement statement = connection.createStatement()) {
            statement.executeUpdate(String.format("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM information_schema.tables " +
                    "WHERE table_schema = 'public' AND table_name = '%s') THEN CREATE TABLE %s(agency text, " +
                    "title text, published_date timestamp without time zone, description text, author text); " +
                    "END IF; END $$;", name, name));
            searchTitle = searchTitleConnection.prepareStatement(String.format("SELECT * FROM %s WHERE title ~ ?;", name
            ));
            searchTitleInDate = searchTitleInDateConnection.prepareStatement(String.format("SELECT * FROM %s WHERE " +
                    "title ~ ? AND published_date >= ? AND published_date <= ?;", name));
            searchDescriptionInDate = searchDescriptionInDateConnection.prepareStatement(String.format("SELECT * " +
                    "FROM %s WHERE description ~ ? AND published_date >= ? AND published_date <= ?;", name));
            searchOnTitleInSpecificSite = this.searchOnTitleInSpecificSiteConnection.prepareStatement(
                    "SELECT * FROM ? WHERE agency = ? AND title LIKE '%?%';");
            searchOnContentInSpecificSite = searchOnContentInSpecificSiteConnection.prepareStatement("SELECT * " +
                    "FROM ? WHERE agency = ? AND description LIKE '%?%'");
            searchOnContent = searchOnContentConnection.prepareStatement("SELECT * FROM ? WHERE description " +
                    "LIKE '%?%'");
        }
        this.name = name;
    }

    public void insert(String agencyName, String title, Date publishedDate, String description, String author) throws
            SQLException {
        if (agencyName.isEmpty())
            agencyName = " ";
        if (title.isEmpty())
            title = " ";
        if (description.isEmpty())
            description = " ";
        if (author.isEmpty())
            author = " ";
        try (final Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             final Statement statement = connection.createStatement()) {
            statement.executeUpdate(String.format("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM %s WHERE agency = $one$%s$" +
                    "one$ AND title = $two$%s$two$) THEN INSERT INTO %s(agency, title, published_date, description, " +
                    "author) VALUES ($three$%s$three$, $four$%s$four$, TIMESTAMP (6) $five$%s$five$, $six$%s$six$, $" +
                    "seven$%s$seven$); END IF; END $$;", name, agencyName, title, name, agencyName, title, new Timestamp
                    (publishedDate.getTime()), description, author));
        }
    }

    public ResultSet searchTitle(final String title) throws SQLException {
        searchTitle.setString(1, title);
        return searchTitle.executeQuery();
    }

    public ResultSet searchTitleInDate(final String title, final Date from, final Date to) throws SQLException {
        searchTitleInDate.setString(1, title);
        searchTitleInDate.setTimestamp(2, new Timestamp(from.getTime()));
        searchTitleInDate.setTimestamp(3, new Timestamp(to.getTime()));
        return searchTitleInDate.executeQuery();
    }

    public ResultSet searchDescriptionInDate(String description, Date from, Date to) throws SQLException {
        searchDescriptionInDate.setString(1, description);
        searchDescriptionInDate.setTimestamp(2, new Timestamp(from.getTime()));
        searchDescriptionInDate.setTimestamp(3, new Timestamp(to.getTime()));
        return searchDescriptionInDate.executeQuery();
    }

    public ResultSet searchOnTitleInSpecificSite(String agencyName, String title) throws SQLException {
        searchOnTitleInSpecificSite.setString(1, name);
        searchOnTitleInSpecificSite.setString(2, agencyName);
        searchOnTitleInSpecificSite.setString(3, title);
        return searchOnTitleInSpecificSite.executeQuery();
    }

    public ResultSet searchOnContentInSpecificSite(String agencyName, String content) throws SQLException {
        searchOnContentInSpecificSite.setString(1, name);
        searchOnContentInSpecificSite.setString(2, agencyName);
        searchOnContentInSpecificSite.setString(3, content);
        return searchOnContentInSpecificSite.executeQuery();
    }

    public ResultSet searchOnContent(String content) throws SQLException {
        searchOnContent.setString(1, name);
        searchOnContent.setString(2, content);
        return searchOnContent.executeQuery();
    }

    public void close() throws SQLException {
        searchTitleConnection.close();
        searchTitleInDateConnection.close();
        searchDescriptionInDateConnection.close();
        searchOnTitleInSpecificSiteConnection.close();
        searchOnContentInSpecificSiteConnection.close();
        searchOnContent.close();
        searchTitle.close();
        searchTitleInDate.close();
        searchDescriptionInDate.close();
        searchOnTitleInSpecificSite.close();
        searchOnContentInSpecificSite.close();
        searchOnContent.close();
    }
}
