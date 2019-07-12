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
    private PreparedStatement allNews;
    private String url;
    private String user;
    private String password;
    private Connection searchTitleConnection;
    private Connection searchTitleInDateConnection;
    private Connection searchDescriptionInDateConnection;
    private Connection searchOnTitleInSpecificSiteConnection;
    private Connection searchOnContentInSpecificSiteConnection;
    private Connection searchOnContentConnection;
    private Connection allNewsConnection;
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
        allNewsConnection = DriverManager.getConnection(url, user, password);
        try (Connection connection = DriverManager.getConnection(url, user, password);
             PreparedStatement prestatement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS " + name +
                     "(agency text, title text, published_date timestamp (6) without time zone," +
                     " description text, author text);")) {
            prestatement.execute();
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
                     "agency = ? and title = ?", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
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
        searchTitle = searchTitleConnection.prepareStatement("SELECT * FROM " + name + " WHERE title ~ ? OFFSET ? ROWS " +
                "FETCH NEXT ? ROWS ONLY;", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        searchTitle.setString(1, title);
        searchTitle.setInt(2, offset);
        searchTitle.setInt(3, count);
        return searchTitle.executeQuery();
    }

    public ResultSet searchTitleInDate(final String title, final Date from, final Date to, int offset, int count) throws SQLException {
        if (searchTitleInDateConnection.isClosed())
            searchTitleInDateConnection = DriverManager.getConnection(url, user, password);
        searchTitleInDate = searchTitleInDateConnection.prepareStatement("SELECT * FROM " + name + " WHERE " +
                "title ~ ? AND published_date >= ? AND published_date <= ? " +
                OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
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
        searchDescriptionInDate = searchDescriptionInDateConnection.prepareStatement("SELECT * " +
                "FROM " + name + " WHERE description ~ ? AND published_date >= ? AND published_date <= ? " +
                OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
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
        searchOnTitleInSpecificSite = searchOnTitleInSpecificSiteConnection.prepareStatement(
                "SELECT * FROM " + name + " WHERE agency = ? AND title ~ ? " +
                        OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        searchOnTitleInSpecificSite.setString(1, agencyName);
        searchOnTitleInSpecificSite.setString(2, title);
        searchOnTitleInSpecificSite.setInt(3, offset);
        searchOnTitleInSpecificSite.setInt(4, count);
        return searchOnTitleInSpecificSite.executeQuery();
    }

    public ResultSet searchOnContentInSpecificSite(String agencyName, String content, int offset, int count) throws SQLException {
        if (searchOnContentInSpecificSiteConnection.isClosed())
            searchOnContentInSpecificSiteConnection = DriverManager.getConnection(url, user, password);
        searchOnContentInSpecificSite = searchOnContentInSpecificSiteConnection.prepareStatement(
                "SELECT * FROM " + name + " WHERE agency = ? AND description ~ ? " +
                        OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        searchOnContentInSpecificSite.setString(1, agencyName);
        searchOnContentInSpecificSite.setString(2, content);
        searchOnContentInSpecificSite.setInt(3, offset);
        searchOnContentInSpecificSite.setInt(4, count);
        return searchOnContentInSpecificSite.executeQuery();
    }

    public ResultSet searchOnContent(String content, int offset, int count) throws SQLException {
        if (searchOnContentConnection.isClosed())
            searchOnContentConnection = DriverManager.getConnection(url, user, password);
        searchOnContent = searchOnContentConnection.prepareStatement("SELECT * FROM " + name + " WHERE description ~ ? "
                + OFFSET_ROWS_FETCH_NEXT_ROWS_ONLY, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
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
        try {
            searchTitle.close();
            searchTitleInDate.close();
            searchDescriptionInDate.close();
            searchOnTitleInSpecificSite.close();
            searchOnContentInSpecificSite.close();
            searchOnContent.close();
            allNews.close();
        } catch (NullPointerException ignored) {}
    }

    private void closeConnections() throws SQLException {
        try {
            searchTitleConnection.close();
            searchTitleInDateConnection.close();
            searchDescriptionInDateConnection.close();
            searchOnTitleInSpecificSiteConnection.close();
            searchOnContentInSpecificSiteConnection.close();
            searchOnContentConnection.close();
            allNewsConnection.close();
        } catch (NullPointerException ignored) {}
    }

    public int sizeOfAllNews() throws SQLException {
        if (allNewsConnection.isClosed())
            allNewsConnection = DriverManager.getConnection(url, user, password);
        allNews = allNewsConnection.prepareStatement("select * from " + name + ";"
                , ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        return App.resultSetSize(allNews.executeQuery());
    }
}
