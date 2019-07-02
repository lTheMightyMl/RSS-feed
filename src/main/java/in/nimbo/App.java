package in.nimbo;

import com.rometools.rome.feed.synd.SyndContent;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;

import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Consumer;

public class App {
    private static final String RSS_FEED = "jdbc:postgresql://localhost:5433/postgres";
    private static final String USER = "postgres";
    private static final String PASSWORD = "sahab";
    private static final String NEWS_AGENCIES = "newsAgencies";
    private static final String TABLENAME = "news";

    public static void main(String[] args) throws SQLException {
        init();
    }

    private static void init() throws SQLException {

//        Connection connection = DriverManager.getConnection(RSS_FEED, USER, PASSWORD);
//        connection.setAutoCommit(false);
//        Statement statement = connection.createStatement();
//        final HashMap<String, String> newsAgencies = new HashMap<>();
//        try (ResultSet resultSet = statement.executeQuery("SELECT EXISTS \n" +
//                "(\n" +
//                "\tSELECT 1\n" +
//                "\tFROM information_schema.tables \n" +
//                "\tWHERE table_schema = 'public'\n" +
//                "\tAND table_name = "+ TABLENAME +"\n" +
//                ");")) {
//        } catch (SQLException e) {
//            initDefaultValues(newsAgencies);
//        }

        ArrayList<New> news = new ArrayList<>();

        news.addAll(IrnaReader.getNews());

    }

    private static void initDefaultValues(HashMap<String, String> newsAgencies) {
//        newsAgencies.put("Tasnim News", "https://www.tasnimnews.com/fa/rss/feed/0/7/0/آخرین-اخبار-اخبار-روز");

//        newsAgencies.put("ISNA", "https://www.isna.ir/rss");

//        newsAgencies.put("SNN", "https://snn.ir/fa/rss/allnews");

//        newsAgencies.put("Mehr News", "feed:https://www.mehrnews.com/rss");
    }
}

// ci travis