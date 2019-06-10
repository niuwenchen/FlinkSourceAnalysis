package com.jackniu.flink.configuration.description;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JackNiu on 2019/6/6.
 */
public class Description {
    private final List<BlockElement> blocks;

    public static DescriptionBuilder builder() {
        return new DescriptionBuilder();
    }

    public List<BlockElement> getBlocks() {
        return blocks;
    }

    public static class DescriptionBuilder{
        private final List<BlockElement> blocks = new ArrayList<BlockElement>();

        public DescriptionBuilder text(String format,InlineElement... elements){
            blocks.add(TextElement.text(format,elements));
            return this;
        }

        public DescriptionBuilder text(String text) {
            blocks.add(TextElement.text(text));
            return this;
        }

        public DescriptionBuilder add(BlockElement block) {
            blocks.add(block);
            return this;
        }

        public DescriptionBuilder linebreak() {
            blocks.add(LineBreakElement.linebreak());
            return this;
        }

        public DescriptionBuilder list(InlineElement... elements) {
            blocks.add(ListElement.list(elements));
            return this;
        }

        public Description build() {
            return new Description(blocks);
        }

    }
    private Description(List<BlockElement> blocks) {
        this.blocks = blocks;
    }



}
