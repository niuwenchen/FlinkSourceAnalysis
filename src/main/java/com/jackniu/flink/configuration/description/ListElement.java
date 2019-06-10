package com.jackniu.flink.configuration.description;


import java.util.Arrays;
import java.util.List;

/**
 * Created by JackNiu on 2019/6/10.
 */
public class ListElement implements BlockElement {

    private final List<InlineElement> entries;

    public static ListElement list(InlineElement... elements) {
        return new ListElement(Arrays.asList(elements));
    }

    public List<InlineElement> getEntries() {
        return entries;
    }

    private ListElement(List<InlineElement> entries) {
        this.entries = entries;
    }


    @Override
    public void format(Formatter formatter) {
        formatter.format(this);
    }

}
