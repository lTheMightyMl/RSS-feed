package in.nimbo;

import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;
import in.nimbo.database.Table;

import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Date;

import static in.nimbo.App.LOGGER;


public class ProcessAgencie implements Runnable {

    private Table table;
    private String agencyName, agencyURL;


    ProcessAgencie(Table table, String agencyName, String agencyURL) {
        this.table = table;
        this.agencyName = agencyName;
        this.agencyURL = agencyURL;
    }

    @Override
    public void run() {
        LOGGER.info(agencyName);
        SyndFeed feed = null;
        try {
            feed = new SyndFeedInput().build(new XmlReader(new URL(agencyURL)));
        } catch (FeedException | IOException e) {
            e.printStackTrace();
        }
        feed.getEntries().forEach(entry -> {
            String title = entry.getTitle();
            Date publishedDate = entry.getPublishedDate();
            String description = entry.getDescription().getValue();
            String author = entry.getAuthor();
            try {
                table.insert(agencyName, title, publishedDate, description, author);
            } catch (SQLException e) {
                System.out.println("error");
            }
        });
    }
}
