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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App {
    private static final String TEXT = "(\\w+)";
    private static final String TITLE = TEXT;
    private static final String DESCRIPTION = TEXT;
    private static final String DATE = "(\\d{4}\\\\\\d{2}\\\\\\d{2})";
    private static final String FROM_DATE_TO_DATE = DATE + "\\s+" + DATE;
    private static final String TITLE_LITERAL = "title";
    private static final String SEARCH_TITLE = TITLE_LITERAL + "\\s+" + TITLE;
    private static final String SEARCH_TITLE_AND_DATE = TITLE_LITERAL + "\\s+" + TITLE + "\\s+" + FROM_DATE_TO_DATE;
    private static final String DESCRIPTION_LITERAL = "description";
    private static final String SEARCH_DESCRIPTION_AND_DATE = DESCRIPTION_LITERAL + "\\s+" + DESCRIPTION + "\\s+" +
            FROM_DATE_TO_DATE;
    private static final String EXIT = "exit";
    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);
    private static final Scanner SCANNER = new Scanner(System.in);
    private static final String PUBLISHED_DATE = "published_date";
    private static final String AUTHOR = "author";
    private static final String TABLE;
    private static final String AGENCY = "agency";

    static {
        Properties databaseProperties = new Properties();
        try {
            databaseProperties.load(new FileInputStream(Objects.requireNonNull(Thread.currentThread().
                    getContextClassLoader().getResource("database.properties")).getPath()));
        } catch (IOException e) {
            LOGGER.error("", e);
            System.exit(0);
        }
        TABLE = databaseProperties.getProperty("table");
    }

    public static void main(String[] args) {
        final Table rssFeeds;
        try {
            rssFeeds = new Table(TABLE);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> rssFeeds.close()));
            writeToDB(rssFeeds);
            String command = SCANNER.nextLine().trim();
            while (!command.matches(EXIT)) {
                decide(rssFeeds, command);
                command = SCANNER.nextLine().trim();
            }
        } catch (SQLException | IOException | ParseException e) {
            LOGGER.error("", e);
        }
    }

    private static void decide(Table rssFeeds, String command) throws SQLException, ParseException {
        if (command.matches(SEARCH_TITLE))
            searchTitle(rssFeeds, command);
        else if (command.matches(SEARCH_TITLE_AND_DATE))
            searchTitleInDate(rssFeeds, command);
        else if (command.matches(SEARCH_DESCRIPTION_AND_DATE))
            searchDescriptionInDate(rssFeeds, command);
    }

    private static void searchDescriptionInDate(final Table rssFeeds, final String command) throws ParseException,
            SQLException {
        Matcher matcher = Pattern.compile(SEARCH_DESCRIPTION_AND_DATE).matcher(command);
        while (matcher.find())
            printResultSet(rssFeeds.searchDescriptionInDate(matcher.group(1), toDate(matcher.group(2)), toDate(matcher.
                    group(3))));
    }

    private static void printResultSet(ResultSet resultSet) throws SQLException {
        while (resultSet.next())
            printFeed(resultSet.getString(AGENCY), resultSet.getString(TITLE_LITERAL), new Date(resultSet.getTimestamp(PUBLISHED_DATE).getTime()),
                    resultSet.getString(DESCRIPTION_LITERAL), resultSet.getString(AUTHOR));
    }

    private static void searchTitleInDate(final Table rssFeeds, final String command) throws ParseException,
            SQLException {
        Matcher matcher = Pattern.compile(SEARCH_TITLE_AND_DATE).matcher(command);
        while (matcher.find())
            printResultSet(rssFeeds.searchTitleInDate(matcher.group(1), toDate(matcher.group(2)), toDate(matcher.group(3
            ))));
    }

    private static Date toDate(String date) throws ParseException {
        return new SimpleDateFormat("yyyy\\MM\\dd").parse(date);
    }

    private static void searchTitle(final Table rssFeeds, final String command) throws SQLException {
        final Matcher matcher = Pattern.compile(SEARCH_TITLE).matcher(command);
        while (matcher.find()) {
            final ResultSet resultSet = rssFeeds.searchTitle(matcher.group(1));
            printResultSet(resultSet);
        }
    }

    private static void printFeed(final String agency, final String title, final Date publishedDate, final String description, final String
            author) {
        LOGGER.info(agency);
        LOGGER.info(title);
        final String publishedDateString = publishedDate.toString();
        LOGGER.info(publishedDateString);
        LOGGER.info(description);
        LOGGER.info(author);
    }

    private static void writeToDB(final Table rssFeeds) throws IOException {
        Properties newsAgencies = loadAgencies();
        Enumeration<?> agencyNames = newsAgencies.propertyNames();
        final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(newsAgencies.size());
        while (agencyNames.hasMoreElements()) {
            Object agency = agencyNames.nextElement();
            scheduledThreadPoolExecutor.scheduleWithFixedDelay(() -> {
                try {
                    processAgency(rssFeeds, agency.toString(), newsAgencies.getProperty(agency.toString()));
                } catch (FeedException | IOException e) {
                    LOGGER.error("", e);
                }
            }, 0, 200, TimeUnit.MILLISECONDS);
        }
    }

    private static void processAgency(final Table table, final String agencyName, final String agencyURL) throws
            FeedException, IOException {
//        LOGGER.info(agencyName);
        SyndFeed feed = new SyndFeedInput().build(new XmlReader(new URL(agencyURL)));
        feed.getEntries().forEach(entry -> processEntry(table, agencyName, entry));
    }

    private static void processEntry(final Table table, final String agencyName, final SyndEntry entry) {
        String title = entry.getTitle();
        Date publishedDate = entry.getPublishedDate();
        String description = entry.getDescription().getValue();
        String author = entry.getAuthor();
        try {
            table.insert(agencyName, title, publishedDate, description, author);
        } catch (SQLException e) {
            LOGGER.error("", e);
        }
    }

    private static Properties loadAgencies() throws IOException {
        Properties newsAgencies = new Properties();
        newsAgencies.load(new FileInputStream(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().
                getResource("news" +
                        "Agencies.properties")).getPath()));
        return newsAgencies;
    }
}