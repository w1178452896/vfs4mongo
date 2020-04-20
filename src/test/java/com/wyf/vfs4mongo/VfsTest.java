package com.wyf.vfs4mongo;

import com.alibaba.fastjson.JSONArray;
import com.wyf.utils.FileFunc;
import com.wyf.vfs.VfsFile;
import com.wyf.vfs4mongo.impl.VfsFile4Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

@RunWith(SpringRunner.class)
@SpringBootTest
public class VfsTest {
    @Autowired
    VfsFile vfsFile;
    @Autowired
    MongoTemplate mongoTemplate;
    @Autowired
    MongoDbFactory mongoDbFactory;
    @Autowired
    MongoClient mongoclient;
    @Autowired
    MongoTransactionManager mongoTransactionManager;

    @Test
    public void test(){
        vfsFile.startTransaction();

        vfsFile.setFileAsString("/王耀锋/11www/aa.txt","hahahah12");
        vfsFile.setFileAsString("/王耀锋/11www/bbb.ini","hahahah12");
        VfsFile ss = this.vfsFile.getVfsFile("/王耀锋/11www/gbk.txt");
        ss.saveAsString("this is a gbk file!","GBK");
        vfsFile.setFileAsString("/王耀锋/22www/cccc.ini","cccccccccccccc");
        vfsFile.commitTransaction();
        vfsFile.endTransaction();
//        vfsFile.setAsBytes();
    }
    @Test
    public void test2(){
        String a="11/33/";
        System.out.println(a.indexOf("11/"));
        String s = a.replaceFirst("11/", "22/");
        System.out.println(s);
        System.out.println(a.substring("11/".length()));
    }
    @Test
    public void testSearch(){
        ClientSession clientSession = mongoclient.startSession();
        MongoDatabase vfsTest = mongoclient.getDatabase("vfsTest");
        MongoCollection<Document> a = vfsTest.getCollection("person");
        MongoCollection<Document> a2 = vfsTest.getCollection("person2");
        clientSession.startTransaction();
        a.insertOne(clientSession,new Document("a","b"));
        a2.insertOne(clientSession,new Document("a","b"));
        clientSession.abortTransaction();
        clientSession.close();
    }
    public void test212(){
        ClientSession clientSession = mongoclient.startSession();
//        Document document = new Document();
//        document.
//        new BasicQuery();
//        mongoTemplate.find();
        mongoTemplate.withSession(clientSession).execute(action -> {
            return null;
        });

    }

    public void testUNzip(){
        Path path = Paths.get("");

    }
    @Test
    public  void testFasetJson() {
        VfsFile vfsFile = this.vfsFile.getVfsFile("/root");
        VfsFile[] vfsFiles = {vfsFile};
        System.out.println(JSONArray.toJSONStringWithDateFormat(vfsFiles, "yyyy-MM-dd HH:mm:ss"));
    }
    @Test
    public void testBytesCharset() throws IOException {
        String file = "C:\\Users\\wangyaofeng\\Desktop/idea快捷键.txt";
        byte[] bytes = file2byte(file);
        //编码判断
        String encoding = FileFunc.getEncoding(bytes);
        System.out.println("字符编码是：" + encoding);
        System.out.println("原乱码输出：" + new String(bytes));
        System.out.println("//***********************//");
        System.out.println("根据文件编码输出：" + new String(bytes, encoding));
    }

    public static byte[] file2byte(String filePath) throws IOException {
        byte[] buffer = null;
        try {
            File file = new File(filePath);
            FileInputStream fis = new FileInputStream(file);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] b = new byte[1024];
            int n;
            while ((n = fis.read(b)) != -1) {
                bos.write(b, 0, n);
            }
            fis.close();
            bos.close();
            buffer = bos.toByteArray();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return buffer;
    }

    @Test
    public void testIndex(){
        IndexOperations indexOperations = mongoTemplate.indexOps(VfsFile4Mongo.class);
        Document document = new Document();
        document.put("parentDir",1);
        document.put("name",1);
        indexOperations.ensureIndex(new CompoundIndexDefinition(document).collation(Collation.from(new Document("unique",true)))) ;
    }
}
