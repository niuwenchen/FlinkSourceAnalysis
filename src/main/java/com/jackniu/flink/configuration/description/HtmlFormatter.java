package com.jackniu.flink.configuration.description;

import java.util.EnumSet;

/**
 * Created by JackNiu on 2019/6/10.
 */
public class HtmlFormatter extends Formatter {


    protected void formatLink(StringBuilder state,String link,String description)
    {
        state.append(String.format("<a href=\"%s\">%s</a>", link, description));
    }

    protected void formatLineBreak(StringBuilder state) {
        state.append("<br/>");
    }

    @Override
    protected void formatText(
            StringBuilder state,
            String format,
            String[] elements,
            EnumSet<TextElement.TextStyle> styles) {
        String escapedFormat = escapeCharacters(format);

        String prefix = "";
        String suffix = "";
        if (styles.contains(TextElement.TextStyle.CODE)) {
            prefix = "<span markdown=\"span\">`";
            suffix = "`</span>";
        }
        state.append(prefix);
        state.append(String.format(escapedFormat, elements));
        state.append(suffix);
    }

    @Override
    protected void formatList(StringBuilder state, String[] entries) {
        state.append("<ul>");
        for (String entry : entries) {
            state.append(String.format("<li>%s</li>", entry));
        }
        state.append("</ul>");
    }

    @Override
    protected Formatter newInstance() {
        return new HtmlFormatter();
    }

    private static String escapeCharacters(String value) {
        return value
                .replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;");
    }
}
