package com.gs.kafka.pojo;

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

/*    public String getA() {
        return a;
    }

    public void setA(String a) {
        this.a = a;
    }

    public String getB() {
        return b;
    }

    public void setB(String b) {
        this.b = b;
    }

    public String getC() {
        return c;
    }
*/
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