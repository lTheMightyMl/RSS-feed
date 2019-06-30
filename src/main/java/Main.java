import java.sql.*;
import java.util.HashMap;

public class Main {
    private static final String RSS_FEED = "jdbc:postgresql://localhost:5432/rss_feed";
    private static final String USER = "smsk";
    private static final String PASSWORD = "";
    private static final String NEWS_AGENCIES = "NEWS_AGENCIES";

    public static void main(String[] args) throws SQLException {
        init();
    }

    private static void init() throws SQLException {
        Connection connection = DriverManager.getConnection(RSS_FEED, USER, PASSWORD);
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();
        try (ResultSet resultSet = statement.executeQuery("SELECT * FROM " + NEWS_AGENCIES + ";")) {
            System.out.println(resultSet.getFetchSize());
            while (resultSet.next())
                System.out.println(resultSet.getString("name"));
        } catch (SQLException e) {
            final HashMap<String, String> newsAgencies = new HashMap<>();
            initDefaultValues(newsAgencies);
        }
    }
        private static void initDefaultValues (HashMap < String, String > newsAgencies){
            newsAgencies.put("Tasnim News", "https://www.tasnimnews.com/fa/rss/feed/0/7/0/آخرین-اخبار-اخبار-روز");

            newsAgencies.put("ISNA", "https://www.isna.ir/rss");

            newsAgencies.put("IRNA", "https://www.irna.ir/rss");

            newsAgencies.put("SNN", "https://snn.ir/fa/rss/allnews");

            newsAgencies.put("Mehr News", "feed:https://www.mehrnews.com/rss");
        }
    }