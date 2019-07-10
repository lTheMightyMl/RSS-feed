package in.nimbo.database;

import in.nimbo.ExternalData;

import java.sql.*;
import java.util.Date;

public class Table {
    private static final String OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY = "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;";
    private final PreparedStatement searchTitle;
    private final PreparedStatement searchTitleInDate;
    private final PreparedStatement searchDescriptionInDate;
    private final PreparedStatement searchOnTitleInSpecificSite;
    private final PreparedStatement searchOnContentInSpecificSite;
    private final PreparedStatement searchOnContent;
    private String url;
    private String user;
    private String password;
    private Connection searchTitleConnection;
    private Connection searchTitleInDateConnection;
    private Connection searchDescriptionInDateConnection;
    private Connection searchOnTitleInSpecificSiteConnection;
    private Connection searchOnContentInSpecificSiteConnection;
    private Connection searchOnContentConnection;
    private String name;

    public Table(String name, ExternalData probs) throws SQLException {
        if (name == null || name.isEmpty()) {
            name = "default";
        }
        url = probs.getPropertyValue("url");
        user = probs.getPropertyValue("user");
        password = probs.getPropertyValue("password");
        searchTitleConnection = DriverManager.getConnection(url, user, password);
        searchTitleInDateConnection = DriverManager.getConnection(url, user, password);
        searchDescriptionInDateConnection = DriverManager.getConnection(url, user, password);
        searchOnTitleInSpecificSiteConnection = DriverManager.getConnection(url, user, password);
        searchOnContentInSpecificSiteConnection = DriverManager.getConnection(url, user, password);
        searchOnContentConnection = DriverManager.getConnection(url, user, password);
        try (Connection connection = DriverManager.getConnection(url, user, password);
             PreparedStatement prestatement = connection.prepareStatement("DO $$ BEGIN IF NOT EXISTS(SELECT 1 " +
                     "FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '" + name + "') " +
                     "THEN CREATE TABLE " + name + "(agency text, title text, published_date timestamp (6) without " +
                     "time zone, description text, author text); END IF; END $$;")
        ) {
            prestatement.executeUpdate();
            searchTitle = searchTitleConnection.prepareStatement("SELECT * FROM " + name + " WHERE title ~ ? " +
                    "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;");
            searchTitleInDate = searchTitleInDateConnection.prepareStatement("SELECT * FROM " + name + " WHERE " +
                    "title ~ ? AND published_date >= ? AND published_date <= ? " + OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY);
            searchDescriptionInDate = searchDescriptionInDateConnection.prepareStatement("SELECT * " +
                    "FROM " + name + " WHERE description ~ ? AND published_date >= ? AND published_date <= ? " +
                    OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY);
            searchOnTitleInSpecificSite = searchOnTitleInSpecificSiteConnection.prepareStatement(
                    "SELECT * FROM " + name + " WHERE agency = ? AND title ~ ? " +
                            OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY);
            searchOnContentInSpecificSite = searchOnContentInSpecificSiteConnection.prepareStatement(
                    "SELECT * FROM " + name + " WHERE agency = ? AND description ~ ? " +
                            OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY);
            searchOnContent = searchOnContentConnection.prepareStatement("SELECT * FROM " + name + " WHERE " +
                    "description ~ ? OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;");
        }
        this.name = name;
    }

    public void insert(String agencyName, String title, Date publishedDate, String description, String author) throws
            SQLException {
        if (agencyName == null || agencyName.isEmpty())
            agencyName = " ";
        if (title == null || title.isEmpty())
            title = " ";
        if (publishedDate == null)
            publishedDate = new Date(0);
        if (description == null || description.isEmpty())
            description = " ";
        if (author == null || author.isEmpty())
            author = " ";
        try (Connection connection = DriverManager.getConnection(url, user, password);
             PreparedStatement prepstatement = connection.prepareStatement("DO $$ BEGIN IF NOT EXISTS(SELECT 1 " +
                     "FROM " + name + " WHERE agency = $one$" + agencyName + "$one$ AND title = $two$" + title +
                     "$two$) THEN INSERT INTO " + name + "(agency, title, published_date, description, author) " +
                     "VALUES ($three$" + agencyName + "$three$, $four$" + title + "$four$, TIMESTAMP (6) WITH TIME " +
                     "ZONE $five$" + new Timestamp(publishedDate.getTime()) + "$five$, $six$" + description +
                     "$six$, $seven$" + author + "$seven$); END IF; END $$;")) {
            prepstatement.executeUpdate();
        }
    }

    public ResultSet searchTitle(String title, int offset, int count) throws SQLException {
        if (searchTitleConnection.isClosed())
            searchTitleConnection = DriverManager.getConnection(url, user, password);
        searchTitle.setString(1, title);
        searchTitle.setInt(2, offset);
        searchTitle.setInt(3, count);
        return searchTitle.executeQuery();
    }

    public ResultSet searchTitleInDate(final String title, final Date from, final Date to, int offset, int count) throws SQLException {
        if (searchTitleInDateConnection.isClosed())
            searchTitleInDateConnection = DriverManager.getConnection(url, user, password);
        searchTitleInDate.setString(1, title);
        searchTitleInDate.setTimestamp(2, new Timestamp(from.getTime()));
        searchTitleInDate.setTimestamp(3, new Timestamp(to.getTime()));
        searchTitleInDate.setInt(4, offset);
        searchTitleInDate.setInt(5, count);
        return searchTitleInDate.executeQuery();
    }

    public ResultSet searchDescriptionInDate(String description, Date from, Date to, int offset, int count) throws SQLException {
        if (searchTitleInDateConnection.isClosed())
            searchTitleInDateConnection = DriverManager.getConnection(url, user, password);
        searchDescriptionInDate.setString(1, description);
        searchDescriptionInDate.setTimestamp(2, new Timestamp(from.getTime()));
        searchDescriptionInDate.setTimestamp(3, new Timestamp(to.getTime()));
        searchDescriptionInDate.setInt(4, offset);
        searchDescriptionInDate.setInt(5, count);
        return searchDescriptionInDate.executeQuery();
    }

    public ResultSet searchOnTitleInSpecificSite(String agencyName, String title, int offset, int count) throws SQLException {
        if (searchOnTitleInSpecificSiteConnection.isClosed())
            searchOnTitleInSpecificSiteConnection = DriverManager.getConnection(url, user, password);
        searchOnTitleInSpecificSite.setString(1, agencyName);
        searchOnTitleInSpecificSite.setString(2, title);
        searchOnTitleInSpecificSite.setInt(3, offset);
        searchOnTitleInSpecificSite.setInt(4, count);
        return searchOnTitleInSpecificSite.executeQuery();
    }

    public ResultSet searchOnContentInSpecificSite(String agencyName, String content, int offset, int count) throws SQLException {
        if (searchOnContentInSpecificSiteConnection.isClosed())
            searchOnContentInSpecificSiteConnection = DriverManager.getConnection(url, user, password);
        searchOnContentInSpecificSite.setString(1, agencyName);
        searchOnContentInSpecificSite.setString(2, content);
        searchOnContentInSpecificSite.setInt(3, offset);
        searchOnContentInSpecificSite.setInt(4, count);
        return searchOnContentInSpecificSite.executeQuery();
    }

    public ResultSet searchOnContent(String content, int offset, int count) throws SQLException {
        if (searchOnContentConnection.isClosed())
            searchOnContentConnection = DriverManager.getConnection(url, user, password);
        searchOnContent.setString(1, content);
        searchOnContent.setInt(2, offset);
        searchOnContent.setInt(3, count);
        return searchOnContent.executeQuery();
    }

    public void close() throws SQLException {
        closeConnections();
        closePreparedStatements();
    }

    private void closePreparedStatements() throws SQLException {
        searchTitle.close();
        searchTitleInDate.close();
        searchDescriptionInDate.close();
        searchOnTitleInSpecificSite.close();
        searchOnContentInSpecificSite.close();
        searchOnContent.close();
    }

    private void closeConnections() throws SQLException {
        searchTitleConnection.close();
        searchTitleInDateConnection.close();
        searchDescriptionInDateConnection.close();
        searchOnTitleInSpecificSiteConnection.close();
        searchOnContentInSpecificSiteConnection.close();
        searchOnContentConnection.close();
    }
}
