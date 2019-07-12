package in.nimbo;

import in.nimbo.database.Table;
import in.nimbo.exception.BadPropertiesFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Pattern SEARCH_TITLE_PATTERN = Pattern.compile(SEARCH_TITLE);
    private static final String SEARCH_TITLE_AND_DATE = TITLE_LITERAL + "\\s+" + TITLE + "\\s+" + FROM_DATE_TO_DATE;
    private static final Pattern SEARCH_TITLE_AND_DATE_PATTERN = Pattern.compile(SEARCH_TITLE_AND_DATE);
    private static final String DESCRIPTION_LITERAL = "description";
    private static final String SEARCH_DESCRIPTION_AND_DATE = DESCRIPTION_LITERAL + "\\s+" + DESCRIPTION + "\\s+" +
            FROM_DATE_TO_DATE;
    private static final Pattern SEARCH_DESCRIPTION_AND_DATE_PATTERN = Pattern.compile(SEARCH_DESCRIPTION_AND_DATE);
    private static final String SEARCH_DESCRIPTION = DESCRIPTION_LITERAL + "\\s+" + DESCRIPTION;
    private static final Pattern SEARCH_DESCRIPTION_PATTERN = Pattern.compile(SEARCH_DESCRIPTION);
    private static final String AGENCY = TEXT;
    private static final String SEARCH_TITLE_AND_AGENCY = TITLE_LITERAL + "\\s+" + TITLE + "\\s+" + AGENCY;
    private static final Pattern SEARCH_TITLE_AND_AGENCY_PATTERN = Pattern.compile(SEARCH_TITLE_AND_AGENCY);
    private static final String SEARCH_DESCRIPTION_AND_AGENCY = DESCRIPTION_LITERAL + "\\s+" + DESCRIPTION + "\\s+" +
            AGENCY;
    private static final String EXIT = "exit";
    private static final Logger LOGGER = LogManager.getLogger(App.class);
    private static final Scanner SCANNER = new Scanner(System.in);
    private static final String PUBLISHED_DATE = "published_date";
    private static final String AUTHOR = "author";
    private static final String NEWRSS = "new_rss\\s+(.+)";
    private static final String AGENCY_LITERAL = "agency";
    private static final int RESULT_COUNT = 10;
    private static final String THERE_IS_STILL_SOME_DATA_FOR_MORE_TYPE_Y = "there is still some data, for more type " +
            "\'Y\'";
    private static ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
    private static final Pattern URL_PATTERN = Pattern.compile("((https?|ftp|gopher|telnet|file):((//)|(\\\\))"
                    + "+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)");

    public static void main(String[] args) {
        ExternalData probs = null;
        try {
            probs = new ExternalData(args[0]);
        } catch (ArrayIndexOutOfBoundsException e) {
            LOGGER.info("Please pass address of properties file as argument");
            LOGGER.error("address of properties missing", e);
            System.exit(0);
        } catch (BadPropertiesFile badPropertiesFile) {
            LOGGER.info("Bad properties file");
            LOGGER.error("database properties missing in properties file", badPropertiesFile);
            System.exit(0);
        } catch (IOException e) {
            LOGGER.info("Wrong file path");
            LOGGER.error("given path for properties files is not exist");
            System.exit(0);
        }
        final Table rssFeeds;
        try {
            rssFeeds = new Table(probs.getPropertyValue("table"), probs);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    rssFeeds.close();
                } catch (SQLException e) {
                    LOGGER.error("", e);
                }
            }));
            writeToDB(rssFeeds, probs);
            String command = "";
            while (!command.matches(EXIT)) {
                LOGGER.info("ready to take orders ...");
                command = SCANNER.nextLine().trim();
                decide(rssFeeds, command, probs);
            }
        } catch (SQLException | IOException | ParseException e) {
            LOGGER.error("", e);
        }
    }

    private static void decide(Table rssFeeds, String command, ExternalData probs) throws SQLException, ParseException,
            IOException {
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
            addNewRss(rssFeeds, command, probs);
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
                    LOGGER.info(THERE_IS_STILL_SOME_DATA_FOR_MORE_TYPE_Y);
                    if (!SCANNER.next().trim().equalsIgnoreCase("y")) {
                        break;
                    }
                }
            }
        }
    }

    private static void searchOnTitleInSpecificSite(Table rssFeeds, String command) throws SQLException {
        Matcher matcher = SEARCH_TITLE_AND_AGENCY_PATTERN.matcher(command);
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
                    LOGGER.info(THERE_IS_STILL_SOME_DATA_FOR_MORE_TYPE_Y);
                    if (!SCANNER.next().trim().equalsIgnoreCase("y")) {
                        break;
                    }
                }
            }
        }
    }

    private static void searchOnContent(Table rssFeeds, String command) throws SQLException {
        Matcher matcher = SEARCH_DESCRIPTION_PATTERN.matcher(command);
        while (matcher.find()) {
            int offset = 0;
            while (true) {
                ResultSet resultSet = rssFeeds.searchOnContent(matcher.group(1), offset, RESULT_COUNT);
                int len = resultSetSize(resultSet);
                printResultSet(resultSet);
                if (len < RESULT_COUNT) {
                    break;
                } else {
                    LOGGER.info(THERE_IS_STILL_SOME_DATA_FOR_MORE_TYPE_Y);
                    if (!SCANNER.next().trim().equalsIgnoreCase("y")) {
                        break;
                    }
                }
            }
        }
    }

    private static void addNewRss(final Table rssFeeds, String command, ExternalData probs) throws IOException {
        String agencyName;
        String rssUrl;
        int index = 7;  // to find name of agency by storing the index of space after prev command
        final HashMap<String, String> agencies = new HashMap<>();

        Matcher matcher = URL_PATTERN.matcher(command);
        while (matcher.find()) {
            rssUrl = matcher.group(1);
            agencyName = command.substring(index, matcher.start()).trim();

            if (agencyName.isEmpty()) {
                LOGGER.info("bad command format: agency name required for every agency");
                return;
            } else {
                agencies.put(agencyName, rssUrl);
            }

            index = matcher.end() + 1;
        }

        for (Map.Entry<String, String> agenc : agencies.entrySet()) {
            LOGGER.info("one rss added");
            scheduledThreadPoolExecutor.scheduleWithFixedDelay(new ProcessAgency(rssFeeds, agenc.getKey(),
                    agenc.getValue()), 0, 20000, TimeUnit.MILLISECONDS);
            probs.addProperty(agenc.getKey(), agenc.getValue());
        }
    }

    private static void searchDescriptionInDate(final Table rssFeeds, final String command) throws ParseException,
            SQLException {
        Matcher matcher = SEARCH_DESCRIPTION_AND_DATE_PATTERN.matcher(command);
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
                    LOGGER.info(THERE_IS_STILL_SOME_DATA_FOR_MORE_TYPE_Y);
                    if (!SCANNER.next().trim().equalsIgnoreCase("y")) {
                        break;
                    }
                }
            }
        }
    }

    private static void printResultSet(ResultSet resultSet) throws SQLException {
        while (resultSet.next())
            printFeed(resultSet.getString(AGENCY_LITERAL), resultSet.getString(TITLE_LITERAL), new Date(resultSet.
                            getTimestamp(PUBLISHED_DATE).getTime()),
                    resultSet.getString(DESCRIPTION_LITERAL), resultSet.getString(AUTHOR));
    }

    private static void searchTitleInDate(final Table rssFeeds, final String command) throws ParseException,
            SQLException {
        Matcher matcher = SEARCH_TITLE_AND_DATE_PATTERN.matcher(command);
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
                    LOGGER.info(THERE_IS_STILL_SOME_DATA_FOR_MORE_TYPE_Y);
                    if (!SCANNER.next().trim().equalsIgnoreCase("y")) {
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
        final Matcher matcher = SEARCH_TITLE_PATTERN.matcher(command);
        while (matcher.find()) {
            int offset = 0;
            while (true) {
                ResultSet resultSet = rssFeeds.searchTitle(matcher.group(1), offset, RESULT_COUNT);
                int len = resultSetSize(resultSet);
                printResultSet(resultSet);
                if (len < RESULT_COUNT) {
                    break;
                } else {
                    LOGGER.info(THERE_IS_STILL_SOME_DATA_FOR_MORE_TYPE_Y);
                    if (!SCANNER.next().trim().equalsIgnoreCase("y")) {
                        break;
                    }
                }
            }
        }
    }

    private static void printFeed(final String agency, final String title, final Date publishedDate, final String
            description, final String
            author) {
        final String publishedDateString = publishedDate.toString();
        LOGGER.info(agency);
        LOGGER.info(title);
        LOGGER.info(publishedDateString);
        LOGGER.info(description);
        LOGGER.info(author);
        LOGGER.info("");
    }

    private static void writeToDB(final Table rssFeeds, ExternalData probs) {
        HashMap<String, String> agencies = probs.getAllAgencies();
        int threadsOfPool = agencies.size();
        if (threadsOfPool > 10) {
            threadsOfPool = 10;
        }
        scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(threadsOfPool);
        for (Map.Entry<String, String> agency : agencies.entrySet()) {
            scheduledThreadPoolExecutor.scheduleWithFixedDelay(new ProcessAgency(rssFeeds, agency.getKey(),
                            agency.getValue()), 0, 20000, TimeUnit.MILLISECONDS);
        }
    }

    public static int resultSetSize(ResultSet resultSet) throws SQLException {
        int len = 0;
        while (resultSet.next()) {
            len ++;
        }
        return len;
    }
}