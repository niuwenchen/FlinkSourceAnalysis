package com.jackniu.flink.configuration.description;

/**
 * Created by JackNiu on 2019/6/6.
 */
public class LinkElement implements InlineElement {
    private final String link;
    private final String text;

    public static LinkElement link(String link,String text){
        return new LinkElement(link,text);
    }
    public static LinkElement link(String link) {
        return new LinkElement(link, link);
    }
    public String getLink() {
        return link;
    }

    public String getText() {
        return text;
    }

    private LinkElement(String link, String text) {
        this.link = link;
        this.text = text;
    }


    public void format(Formatter formatter) {
        formatter.format(this);
    }


}
