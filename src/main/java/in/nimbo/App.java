package in.nimbo;

import com.rometools.rome.io.FeedException;
import in.nimbo.database.Table;
import in.nimbo.exception.BadPropertiesFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
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
    private static final String SEARCH_DESCRIPTION = DESCRIPTION_LITERAL + "\\s+" + DESCRIPTION;
    private static final String AGENCY = TEXT;
    private static final String SEARCH_TITLE_AND_AGENCY = TITLE_LITERAL + "\\s+" + TITLE + "\\s+" + AGENCY;
    private static final String SEARCH_DESCRIPTION_AND_AGENCY = DESCRIPTION_LITERAL + "\\s+" + DESCRIPTION + "\\s+" + AGENCY;
    private static final String EXIT = "exit";
    static final Logger LOGGER = LoggerFactory.getLogger(Table.class);
    private static final Scanner SCANNER = new Scanner(System.in);
    private static final String PUBLISHED_DATE = "published_date";
    private static final String AUTHOR = "author";
    private static final String NEWRSS = "new_rss\\s+(.+)";
    private static final String AGENCY_LITERAL = "agency";
    private static final int RESULT_COUNT = 10;
    private static ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    public static void main(String[] args) {
        try {
            ExternalData.loadProperties(args[0]);
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Please pass address of properties file as argument");
            LOGGER.error("address of properties missing", e);
            System.exit(0);
        } catch (BadPropertiesFile badPropertiesFile) {
            System.out.println("Bad properties file");
            LOGGER.error("database properties missing in properties file", badPropertiesFile);
            System.exit(0);
        } catch (IOException e) {
            System.out.println("Wrong file path");
            LOGGER.error("given path for properties files is not exist");
            System.exit(0);
        }
        final Table rssFeeds;
        try {
            rssFeeds = new Table(ExternalData.getPropertyValue("table"));
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    rssFeeds.close();
                } catch (SQLException e) {
                    LOGGER.error("", e);
                }
            }));
            writeToDB(rssFeeds);
            String command = "";
            while (!command.matches(EXIT)) {
                System.out.println("ready to take orders ...");
                command = SCANNER.nextLine().trim();
                decide(rssFeeds, command);
            }
        } catch (SQLException | IOException | ParseException | FeedException e) {
            LOGGER.error("", e);
        }
    }

    private static void decide(Table rssFeeds, String command) throws SQLException, ParseException, IOException {
        if (command.matches(SEARCH_TITLE))
            searchTitle(rssFeeds, command);
        else if (command.matches(SEARCH_TITLE_AND_DATE))
            searchTitleInDate(rssFeeds, command);
        else if (command.matches(SEARCH_DESCRIPTION_AND_DATE))
            searchDescriptionInDate(rssFeeds, command);
        else if (command.matches(SEARCH_DESCRIPTION))
            searchOnContent(rssFeeds, command);
        else if (command.matches(SEARCH_TITLE_AND_AGENCY))
            searchOnTitleInSpecificSite(rssFeeds, command);
        else if (command.matches(SEARCH_DESCRIPTION_AND_AGENCY))
            searchOnContentInSpecificSite(rssFeeds, command);
        else if (command.matches(NEWRSS))
            addNewRss(rssFeeds, command);
    }

    private static void searchOnContentInSpecificSite(Table rssFeeds, String command) throws SQLException {
        Matcher matcher = Pattern.compile(SEARCH_DESCRIPTION_AND_AGENCY).matcher(command);
        while (matcher.find()) {
            int offset = 0;
            while (true) {
                ResultSet resultSet = rssFeeds.searchOnContentInSpecificSite(matcher.group(2), matcher.group(1),
                        offset, RESULT_COUNT);
                int len = resultSetSize(resultSet);
                printResultSet(resultSet);
                if (len < RESULT_COUNT) {
                    break;
                } else {
                    System.out.println("there is still some data, for more type \'Y\'");
                    if (! SCANNER.next().trim().toLowerCase().equals("y")) {
                        break;
                    }
                }
            }
        }
    }

    private static void searchOnTitleInSpecificSite(Table rssFeeds, String command) throws SQLException {
        Matcher matcher = Pattern.compile(SEARCH_TITLE_AND_AGENCY).matcher(command);
        while (matcher.find()) {
            int offset = 0;
            while (true) {
                ResultSet resultSet = rssFeeds.searchOnTitleInSpecificSite(matcher.group(2), matcher.group(1),
                        offset, RESULT_COUNT);
                int len = resultSetSize(resultSet);
                printResultSet(resultSet);
                if (len < RESULT_COUNT) {
                    break;
                } else {
                    System.out.println("there is still some data, for more type \'Y\'");
                    if (! SCANNER.next().trim().toLowerCase().equals("y")) {
                        break;
                    }
                }
            }
        }
    }

    private static void searchOnContent(Table rssFeeds, String command) throws SQLException {
        Matcher matcher = Pattern.compile(SEARCH_DESCRIPTION).matcher(command);
        while (matcher.find()) {
            int offset = 0;
            while (true) {
                ResultSet resultSet = rssFeeds.searchOnContent(matcher.group(1), offset, RESULT_COUNT);
                int len = resultSetSize(resultSet);
                printResultSet(resultSet);
                if (len < RESULT_COUNT) {
                    break;
                } else {
                    System.out.println("there is still some data, for more type \'Y\'");
                    if (! SCANNER.next().trim().toLowerCase().equals("y")) {
                        break;
                    }
                }
            }
        }
    }

    private static void addNewRss(final Table rssFeeds, String command) throws IOException {
        String agencyName;
        String rssUrl;
        int index = 7;  // to find name of agency by storing the index of space after prev command
        final HashMap<String, String> agencies = new HashMap<>();

        final Pattern urlPattern = Pattern.compile(
                "((https?|ftp|gopher|telnet|file):((//)|(\\\\))"
                        + "+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)");

        Matcher matcher = urlPattern.matcher(command);
        while (matcher.find()) {
            rssUrl = matcher.group(1);
            agencyName = command.substring(index, matcher.start()).trim();

            if (agencyName.isEmpty()) {
                System.out.println("bad command format: agency name required for every agency");
                return;
            } else {
                agencies.put(agencyName, rssUrl);
            }

            index = matcher.end() + 1;
        }

        for (Map.Entry<String, String> agenc : agencies.entrySet()) {
            System.out.println("one rss added");
            scheduledThreadPoolExecutor.scheduleWithFixedDelay(new ProcessAgencie(rssFeeds, agenc.getKey(), agenc.getValue()), 0, 20000, TimeUnit.MILLISECONDS);
            ExternalData.addProperty(agenc.getKey(), agenc.getValue());
        }
    }

    private static void searchDescriptionInDate(final Table rssFeeds, final String command) throws ParseException,
            SQLException {
        Matcher matcher = Pattern.compile(SEARCH_DESCRIPTION_AND_DATE).matcher(command);
        while (matcher.find()) {
            int offset = 0;
            while (true) {
                ResultSet resultSet = rssFeeds.searchDescriptionInDate(matcher.group(1), toDate(matcher.group(2)),
                        toDate(matcher.group(3)), offset, RESULT_COUNT);
                int len = resultSetSize(resultSet);
                printResultSet(resultSet);
                if (len < RESULT_COUNT) {
                    break;
                } else {
                    System.out.println("there is still some data, for more type \'Y\'");
                    if (! SCANNER.next().trim().toLowerCase().equals("y")) {
                        break;
                    }
                }
            }
        }
    }

    private static void printResultSet(ResultSet resultSet) throws SQLException {
        while (resultSet.next())
            printFeed(resultSet.getString(AGENCY_LITERAL), resultSet.getString(TITLE_LITERAL), new Date(resultSet.getTimestamp(PUBLISHED_DATE).getTime()),
                    resultSet.getString(DESCRIPTION_LITERAL), resultSet.getString(AUTHOR));
    }

    private static void searchTitleInDate(final Table rssFeeds, final String command) throws ParseException,
            SQLException {
        Matcher matcher = Pattern.compile(SEARCH_TITLE_AND_DATE).matcher(command);
        while (matcher.find()) {
            int offset = 0;
            while (true) {
                ResultSet resultSet = rssFeeds.searchTitleInDate(matcher.group(1), toDate(matcher.group(2)),
                        toDate(matcher.group(3)), offset, RESULT_COUNT);
                int len = resultSetSize(resultSet);
                printResultSet(resultSet);
                if (len < RESULT_COUNT) {
                    break;
                } else {
                    System.out.println("there is still some data, for more type \'Y\'");
                    if (! SCANNER.next().trim().toLowerCase().equals("y")) {
                        break;
                    }
                }
            }
        }
    }

    private static Date toDate(String date) throws ParseException {
        return new SimpleDateFormat("yyyy\\MM\\dd").parse(date);
    }

    private static void searchTitle(final Table rssFeeds, final String command) throws SQLException {
        final Matcher matcher = Pattern.compile(SEARCH_TITLE).matcher(command);
        while (matcher.find()) {
            int offset = 0;
            while (true) {
                ResultSet resultSet = rssFeeds.searchTitle(matcher.group(1), offset, RESULT_COUNT);
                int len = resultSetSize(resultSet);
                printResultSet(resultSet);
                if (len < RESULT_COUNT) {
                    break;
                } else {
                    System.out.println("there is still some data, for more type \'Y\'");
                    if (! SCANNER.next().trim().toLowerCase().equals("y")) {
                        break;
                    }
                }
            }
        }
    }

    private static void printFeed(final String agency, final String title, final Date publishedDate, final String description, final String
            author) {
        final String publishedDateString = publishedDate.toString();
        LOGGER.info(agency);
        LOGGER.info(title);
        LOGGER.info(publishedDateString);
        LOGGER.info(description);
        LOGGER.info(author);
    }

    private static void writeToDB(final Table rssFeeds) throws IOException, FeedException {
        HashMap<String, String> agencies = ExternalData.getAllAgencies();
        scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(agencies.size());
        for (Map.Entry<String, String> agency : agencies.entrySet()) {
            scheduledThreadPoolExecutor.scheduleWithFixedDelay(new ProcessAgencie(rssFeeds, agency.getKey(), agency.getValue()),
                    0, 20000, TimeUnit.MILLISECONDS);
        }
    }

    private static int resultSetSize(ResultSet resultSet) throws SQLException {
        resultSet.last();
        int len = resultSet.getRow();
        resultSet.beforeFirst();
        return len;
    }
}