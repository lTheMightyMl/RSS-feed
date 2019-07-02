package in.nimbo;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;
import in.nimbo.database.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.Enumeration;
import java.util.Objects;
import java.util.Properties;

public class App {
    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);

    public static void main(String[] args) throws IOException, FeedException, SQLException {
        writeToDB();
    }

    private static void writeToDB() throws IOException, FeedException, SQLException {
        final Table rssFeeds = new Table("rss_feeds");
        Properties newsAgencies = loadAgencies();
        Enumeration<?> agencyNames = newsAgencies.propertyNames();
        while (agencyNames.hasMoreElements()) {
            Object agency = agencyNames.nextElement();
            processAgency(rssFeeds, agency.toString(), newsAgencies.getProperty(agency.toString()));
        }
    }

    private static void processAgency(Table table, String agencyName, String agencyURL) throws FeedException,
            IOException {
        LOGGER.info(agencyName);
        SyndFeed feed = new SyndFeedInput().build(new XmlReader(new URL(agencyURL)));
        feed.getEntries().forEach(entry -> {
            processEntry(table, agencyName, entry);
        });
    }

    private static void processEntry(Table table, String agencyName, SyndEntry entry) {
        String title = entry.getTitle();
        Date publishedDate = entry.getPublishedDate();
        String description = entry.getDescription().getValue();
        String author = entry.getAuthor();
        LOGGER.info(title);
        final String publishedDateString = publishedDate.toString();
        LOGGER.info(publishedDateString);
        LOGGER.info(description);
        LOGGER.info(author);
        LOGGER.info("");
        try {
            table.insert(agencyName, title, publishedDate, description, author);
        } catch (SQLException e) {
            LOGGER.error("", e);
        }
    }

    private static Properties loadAgencies() throws IOException {
        Properties newsAgencies = new Properties();
        newsAgencies.load(new FileInputStream(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("news" +
                "Agencies.properties")).getPath()));
        return newsAgencies;
    }
}