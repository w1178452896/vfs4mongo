package com.wyf.vfs4mongo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

@org.springframework.stereotype.Service
public class Service {
    @Autowired
    MongoTemplate mongoTemplate;
    private  String sa="a";

    public void testDocu(){
        Person person = new Person();
        person.setAge(12);
        person.setName("小光");
        person.setExtValue("height",180);
        person.setExtValue("date",new Date());
        mongoTemplate.insert(person);

    }
    @Transactional
    public void testSear(){
        testDocu();
        testDocu();
    }
}
