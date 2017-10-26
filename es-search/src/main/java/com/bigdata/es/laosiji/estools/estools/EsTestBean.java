package com.bigdata.es.laosiji.estools.estools;

public class EsTestBean {
    private long id;
    private String user;
    private String title;
    private String desc;

    public EsTestBean() {
    }

    public EsTestBean(long id, String user, String title, String desc) {
        this.id = id;
        this.user = user;
        this.title = title;
        this.desc = desc;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
