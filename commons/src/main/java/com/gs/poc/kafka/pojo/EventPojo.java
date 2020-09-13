package com.gs.poc.kafka.pojo;

import java.io.Serializable;

public class EventPojo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String a;
    private String b;
    private String c;
    private String d;

    public EventPojo() {
    }

    public EventPojo(String a, String b, String c, String d) {
        this.a = a;
        this.b = b;
        this.c = c;
        this.d = d;
    }

    public void setA(String a) {
        this.a = a;
    }

    public void setB(String b) {
        this.b = b;
    }

    public void setC(String c) {
        this.c = c;
    }

    public String getA() {
        return a;
    }

    public String getB() {
        return b;
    }

    public String getC() {
        return c;
    }

    public String getD() {
        return d;
    }

    public void setD(String d) {
        this.d = d;
    }

    @Override
    public String toString() {
        return "EventPojo{" +
                "a='" + a + '\'' +
                ", b='" + b + '\'' +
                ", c='" + c + '\'' +
                ", d=" + d +
                '}';
    }
}