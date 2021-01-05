package com.yiqi.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author Wanghy
 * @title: RegexExcludePathFilter
 * @projectName hadoop
 * @description: 用于排除匹配正则表达式的路径
 * @date 2021/1/5
 */
public class RegexExcludePathFilter implements PathFilter {
    private final String regex;

    public RegexExcludePathFilter(String regex) {
        this.regex = regex;
    }

    public boolean accept(Path path) {
        return !path.toString().matches(regex);
    }
}
