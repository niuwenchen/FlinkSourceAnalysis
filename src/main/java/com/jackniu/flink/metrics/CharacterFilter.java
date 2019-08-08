package com.jackniu.flink.metrics;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface CharacterFilter {
    /**
     * Filter the given string and generate a resulting string from it.
     *
     * <p>For example, one implementation could filter out invalid characters from the input string.
     *
     * @param input Input string
     * @return Filtered result string
     */
    String filterCharacters(String input);
}
