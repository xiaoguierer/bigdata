package com.bigdata.es.laosiji.estools.EsDoc;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class User {
    private String name;
    private String home;//家乡
    private double height;//身高
    private int age;
    private Date birthday;

    public User() {
    }

    public User(String name, String home, double height, int age, Date birthday) {
        this.name = name;
        this.home = home;
        this.height = height;
        this.age = age;
        this.birthday = birthday;
    }

    /**
     * 随机生成一个用户信息
     *
     * @return
     */
    public static User getOneRandomUser() {
        return new User("葫芦" + (int) (Math.random() * 1000) + "娃",
                "山西省太原市" + (int) (Math.random() * 1000) + "街道",
                (Math.random() * 1000), (int) (Math.random() * 100),
                new Date(System.currentTimeMillis() - (long) (Math.random() * 100000)));
    }

    /**
     * 随机生成num个用户信息
     *
     * @param num 生成数量
     * @return
     */
    public static List<User> getRandomUsers(int num) {
        List<User> users = new ArrayList();
        if (num < 0) num = 10;
        for (int i = 0; i < num; i++) {
            users.add(new User("葫芦" + (int) (Math.random() * 1000) + "娃",
                    "山西省太原市" + (int) (Math.random() * 1000) + "街道",
                    (Math.random() * 1000), (int) (Math.random() * 100),
                    new Date(System.currentTimeMillis() - (long) (Math.random() * 100000))));
        }

        return users;
    }

    /**
     * 封装对象的Json信息
     *
     * @param user
     * @return
     * @throws IOException
     */
    public static XContentBuilder getXContentBuilder(User user) throws IOException {
        return XContentFactory.jsonBuilder()
                .startObject()
                .field("name", user.getName())//该字段在上面的方法中mapping定义了,所以该字段就有了自定义的属性,比如 age等
                .field("home", user.getHome())
                .field("height", user.getHeight())
                .field("age", user.getAge())
                .field("birthday", user.getBirthday())
                .field("state", "默认属性,mapping中没有定义")//该字段在上面方法中的mapping中没有定义,所以该字段的属性使用es默认的.
                .endObject();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }

    public double getHeight() {
        return height;
    }

    public void setHeight(double height) {
        this.height = height;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

}
