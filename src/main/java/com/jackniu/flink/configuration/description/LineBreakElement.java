package com.jackniu.flink.configuration.description;

/**
 * Created by JackNiu on 2019/6/10.
 */
public class LineBreakElement implements BlockElement {
    public static LineBreakElement linebreak() {
        return new LineBreakElement();
    }

    private LineBreakElement() {
    }

    public void format(Formatter formatter){
        formatter.format(this);
    }
}
