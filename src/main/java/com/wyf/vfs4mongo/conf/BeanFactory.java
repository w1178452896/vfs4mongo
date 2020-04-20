package com.wyf.vfs4mongo.conf;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.gridfs.GridFS;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import javax.servlet.MultipartConfigElement;

@Configuration
@EnableMongoRepositories(basePackages = {"com.wyf.vfs4mongo"})
public class BeanFactory {

    @Autowired
    MongoClient mongoClient;
    @Autowired
    MongoDbFactory mongoDbFactory;

    @Bean
    public GridFS gridFS(){
        DB db = new DB(mongoClient, mongoDbFactory.getDb().getName());
        GridFS gridFS = new GridFS(db);
        return gridFS;
    }
    @Bean
    public GridFSBucket gridFSBucket(){
        GridFSBucket gridFSBucket = GridFSBuckets.create(mongoDbFactory.getDb());

        return gridFSBucket;
    }
    @Bean("fsCollection")
    public MongoCollection<Document> fsCollection(){

        return  mongoDbFactory.getDb().getCollection("fs");
    }

    /**
     * 文件上传配置
     * @return
     */
    @Bean
    public MultipartConfigElement multipartConfigElement() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        //文件最大
        factory.setMaxFileSize("1024MB"); //KB,MB
        /// 设置总上传数据总大小
        factory.setMaxRequestSize("2048MB");
        return factory.createMultipartConfig();
    }
    @Bean
    public MongoTransactionManager mongoTransactionManager(){
        return new MongoTransactionManager(mongoDbFactory);
    }



}
