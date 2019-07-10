package in.nimbo.database;

import in.nimbo.App;
import in.nimbo.ExternalData;

import java.sql.*;
import java.util.Date;

public class Table {
    private static final String OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY = "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;";
    private PreparedStatement searchTitle;
    private PreparedStatement searchTitleInDate;
    private PreparedStatement searchDescriptionInDate;
    private PreparedStatement searchOnTitleInSpecificSite;
    private PreparedStatement searchOnContentInSpecificSite;
    private PreparedStatement searchOnContent;
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
             PreparedStatement prestatement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS " + name +
                     "(agency text, title text, published_date timestamp (6) without time zone," +
                     " description text, author text);")) {
            boolean b = prestatement.execute();
            System.out.println("table created? : " + b);
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
             PreparedStatement pscheck = connection.prepareStatement("select  * from  " + name + " where " +
                     "agency = ? and title = ?");
             PreparedStatement psinsert = connection.prepareStatement("insert into " + name + " values (?, ?, ?, ?, ?)")) {
            pscheck.setString(1, agencyName);
            pscheck.setString(2, title);
            ResultSet resultSet = pscheck.executeQuery();
            if (App.resultSetSize(resultSet) == 0) {
                psinsert.setString(1, agencyName);
                psinsert.setString(2, title);
                psinsert.setTimestamp(3, new Timestamp(publishedDate.getTime()));
                psinsert.setString(4, description);
                psinsert.setString(5, author);
                psinsert.executeUpdate();
            }
        }
    }

    public ResultSet searchTitle(String title, int offset, int count) throws SQLException {
        if (searchTitleConnection.isClosed())
            searchTitleConnection = DriverManager.getConnection(url, user, password);
        searchTitle = searchTitleConnection.prepareStatement("SELECT * FROM ? WHERE title ~ ? OFFSET ? ROWS " +
                "FETCH NEXT ? ROWS ONLY;");
        searchTitle.setString(1, name);
        searchTitle.setString(2, title);
        searchTitle.setInt(3, offset);
        searchTitle.setInt(4, count);
        System.out.println(searchTitle);
        return searchTitle.executeQuery();
    }

    public ResultSet searchTitleInDate(final String title, final Date from, final Date to, int offset, int count) throws SQLException {
        if (searchTitleInDateConnection.isClosed())
            searchTitleInDateConnection = DriverManager.getConnection(url, user, password);
        searchTitleInDate = searchTitleInDateConnection.prepareStatement("SELECT * FROM ? WHERE " +
                "title ~ ? AND published_date >= ? AND published_date <= ? " +
                OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY);
        searchTitleInDate.setString(1, name);
        searchTitleInDate.setString(2, title);
        searchTitleInDate.setTimestamp(3, new Timestamp(from.getTime()));
        searchTitleInDate.setTimestamp(4, new Timestamp(to.getTime()));
        searchTitleInDate.setInt(5, offset);
        searchTitleInDate.setInt(6, count);
        return searchTitleInDate.executeQuery();
    }

    public ResultSet searchDescriptionInDate(String description, Date from, Date to, int offset, int count) throws SQLException {
        if (searchTitleInDateConnection.isClosed())
            searchTitleInDateConnection = DriverManager.getConnection(url, user, password);
        searchDescriptionInDate = searchDescriptionInDateConnection.prepareStatement("SELECT * " +
                "FROM ? WHERE description ~ ? AND published_date >= ? AND published_date <= ? " +
                OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY);
        searchDescriptionInDate.setString(1, name);
        searchDescriptionInDate.setString(2, description);
        searchDescriptionInDate.setTimestamp(3, new Timestamp(from.getTime()));
        searchDescriptionInDate.setTimestamp(4, new Timestamp(to.getTime()));
        searchDescriptionInDate.setInt(5, offset);
        searchDescriptionInDate.setInt(6, count);
        return searchDescriptionInDate.executeQuery();
    }

    public ResultSet searchOnTitleInSpecificSite(String agencyName, String title, int offset, int count) throws SQLException {
        if (searchOnTitleInSpecificSiteConnection.isClosed())
            searchOnTitleInSpecificSiteConnection = DriverManager.getConnection(url, user, password);
        searchOnTitleInSpecificSite = searchOnTitleInSpecificSiteConnection.prepareStatement(
                "SELECT * FROM ? WHERE agency = ? AND title ~ ? " +
                        OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY);
        searchOnTitleInSpecificSite.setString(1, name);
        searchOnTitleInSpecificSite.setString(2, agencyName);
        searchOnTitleInSpecificSite.setString(3, title);
        searchOnTitleInSpecificSite.setInt(4, offset);
        searchOnTitleInSpecificSite.setInt(5, count);
        return searchOnTitleInSpecificSite.executeQuery();
    }

    public ResultSet searchOnContentInSpecificSite(String agencyName, String content, int offset, int count) throws SQLException {
        if (searchOnContentInSpecificSiteConnection.isClosed())
            searchOnContentInSpecificSiteConnection = DriverManager.getConnection(url, user, password);
        searchOnContentInSpecificSite = searchOnContentInSpecificSiteConnection.prepareStatement(
                "SELECT * FROM ? WHERE agency = ? AND description ~ ? " +
                        OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY);
        searchOnContentInSpecificSite.setString(1, name);
        searchOnContentInSpecificSite.setString(2, agencyName);
        searchOnContentInSpecificSite.setString(3, content);
        searchOnContentInSpecificSite.setInt(4, offset);
        searchOnContentInSpecificSite.setInt(5, count);
        return searchOnContentInSpecificSite.executeQuery();
    }

    public ResultSet searchOnContent(String content, int offset, int count) throws SQLException {
        if (searchOnContentConnection.isClosed())
            searchOnContentConnection = DriverManager.getConnection(url, user, password);
        searchOnContent = searchOnContentConnection.prepareStatement("SELECT * FROM ? WHERE " +
                "description ~ ? OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;");
        searchOnContent.setString(1, name);
        searchOnContent.setString(2, content);
        searchOnContent.setInt(3, offset);
        searchOnContent.setInt(4, count);
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
