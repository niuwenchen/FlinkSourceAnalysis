package com.jackniu.flink.configuration.description;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

/**
 * Created by JackNiu on 2019/6/6.
 */
public class TextElement implements BlockElement,InlineElement{
    private final String format;
    private final List<InlineElement> elements;
    private final EnumSet<TextStyle> textStyles = EnumSet.noneOf(TextStyle.class);

    private TextElement(String format, List<InlineElement> elements) {
        this.format = format;
        this.elements = elements;
    }

    public static TextElement text(String format, InlineElement... elements) {
        return new TextElement(format, Arrays.asList(elements));
    }

    public static TextElement text(String format){
        return new TextElement(format,Collections.<InlineElement>emptyList());
    }

    public static TextElement code(String text) {
        TextElement element = text(text);
        element.textStyles.add(TextStyle.CODE);
        return element;
    }

    public String getFormat() {
        return format;
    }

    public List<InlineElement> getElements() {
        return elements;
    }

    public EnumSet<TextStyle> getStyles() {
        return textStyles;
    }

    public void format(Formatter formatter) {
        formatter.format(this);
    }



    public enum TextStyle {
        CODE
    }
}
