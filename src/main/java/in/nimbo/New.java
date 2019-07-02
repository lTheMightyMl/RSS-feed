package in.nimbo;

import java.util.Date;

public class New {

    private String title;
    private String content;
    private String source;
    private Date date;

    New(String title, String content, String source, Date date) {
        this.title = title;
        this.content = content;
        this.source = source;
        this.date = date;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
