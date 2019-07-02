package in.nimbo;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

public class IrnaReader {

    final static String feedSource = "https://www.irna.ir/rss";

    static ArrayList<New> getNews() {

        ArrayList<New> news = new ArrayList<>();
        SyndFeed feed = null;

        try {
            URL urlSource = new URL(feedSource);
            SyndFeedInput input = new SyndFeedInput();
            feed = input.build(new XmlReader(urlSource));
        } catch (FeedException | IOException e) {
            e.printStackTrace();
        }

        for (SyndEntry syndEntry : feed.getEntries()) {

            String st = syndEntry.getDescription().getValue();
            New item = new New(syndEntry.getTitle(), syndEntry.getDescription().getValue(), st.substring(st.indexOf('-') + 2, st.indexOf('-') + 7), syndEntry.getPublishedDate());
            news.add(item);

        }

        return news;

    }
}
