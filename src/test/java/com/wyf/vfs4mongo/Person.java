package com.wyf.vfs4mongo;

import org.apache.catalina.User;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

@Document
public class Person<T> extends BaseBean<User>{

    public static  String a="a";
    @Id
    private String id;

    private String name;

    private int age;

    private Service service=new Service();



    @Transient
    private String aa;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getMap() {
        return map;
    }

    public void setMap(String map) {
        this.map = map;
    }



}
