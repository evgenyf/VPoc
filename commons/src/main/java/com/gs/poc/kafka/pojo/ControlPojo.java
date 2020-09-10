package com.gs.poc.kafka.pojo;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;

import java.io.Serializable;

public class ControlPojo implements Serializable {

    private String a;
    private String b;
    private String c;
    private Integer ttl;

    public ControlPojo() {
    }

    public ControlPojo(String a, String b) {
        this( a, b, null, null );
    }

    public ControlPojo(String a, String b, Integer ttl) {
        this( a, b, null, ttl );
    }

    public ControlPojo(String a, String b, String c, Integer ttl) {
        this.a = a;
        this.b = b;
        this.c = c;
        this.ttl = ttl;
    }

    @SpaceIndex
    public String getA() {
        return a;
    }

    public void setA(String a) {
        this.a = a;
    }

    @SpaceIndex
    public String getB() {
        return b;
    }

    public void setB(String b) {
        this.b = b;
    }

    @SpaceIndex
    @SpaceId(autoGenerate = false)
    public String getC() {
        return c;
    }

    public void setC(String c) {
        this.c = c;
    }

    public Integer getTtl() {
        return ttl;
    }

    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }

    @Override
    public String toString() {
        return "ControlPojo{" +
                "a='" + a + '\'' +
                ", b='" + b + '\'' +
                ", c='" + c + '\'' +
                ", ttl=" + ttl +
                '}';
    }
}