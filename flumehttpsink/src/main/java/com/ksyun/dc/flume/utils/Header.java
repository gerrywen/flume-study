package com.ksyun.dc.flume.utils;

import java.util.Objects;

/**
 * program: flume-study->Header
 * description:
 * author: gerry
 * created: 2020-04-21 10:38
 **/
public class Header {

    private final String name;

    private final String value;

    public Header(String name, String value) {
        if (name == null) {
            throw new IllegalArgumentException("Header name cannot be null");
        }
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public boolean hasSameNameAs(Header header) {
        if (header == null) {
            throw new IllegalArgumentException("Header cannot be null");
        }
        return this.name.equalsIgnoreCase(header.getName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Header header = (Header) o;

        if (!Objects.equals(name, header.name)) return false;
        return Objects.equals(value, header.value);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        return 31 * result + (value != null ? value.hashCode() : 0);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append(name);
        if (value != null) {
            builder.append("=").append(value);
        }
        return builder.toString();
    }
}
