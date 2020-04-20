package com.wyf.vfs4mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.*;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import com.mongodb.session.ClientSession;
import org.bson.*;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.gridfs.GridFsResource;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class VfsApplicationTests {

    @Autowired
    MongoDbFactory mongoDbFactory;

    @Autowired
    MongoTemplate mongoTemplate;
    @Autowired
    GridFsTemplate gridFsTemplate;
    @Autowired
    MongoOperations mongoOperations;
    @Autowired
    MongoClient mongoClient;


    @Test
    public void contextLoads() {
//        MongoDatabase db = mongoDbFactory.getDb();
//        MongoClient mongoClient = new MongoClient("localhost", 27017);
//        GridFSBucket gridFSBucket = GridFSBuckets.create(db);
//        GridFS gridFS = new GridFS(mongoClient.getDB("vfsTest"));

        Document document = new Document();
        document.append("wfilename","高级复杂设计器json格式1.json");
        document.append("wcontentType","json");
        GridFSUploadOptions options = new GridFSUploadOptions()
                .chunkSizeBytes(358400)
                .metadata(document);

        File file = new File("C:\\Users\\wangyaofeng\\Desktop/高级表格设计器工作拆分.xlsx");
        try {
            InputStream inputStream = new FileInputStream(file);
            //gridFSBucket.uploadFromStream(new BsonObjectId(new ObjectId("5cfe2a6761b1c6309c6e1213")),"高级复杂设计器json格式1.json", inputStream, options);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 测试读取gridfs gridFSBucket
     */
    @Test
    public void testRead() throws IOException {
        MongoDatabase db = mongoDbFactory.getDb();
        GridFSBucket gridFSBucket = GridFSBuckets.create(db);
        ClientSession session;
        File file = new File("C:\\Users\\wangyaofeng\\Desktop/haha.json");
        if(!file.exists()){
            file.createNewFile();
        }
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        GridFSDownloadStream gridFSDownloadStream = gridFSBucket.openDownloadStream("高级复杂设计器json格式1.json");
        GridFSFile gridFSFile = gridFSDownloadStream.getGridFSFile();
        GridFSDBFile gridFSDBFile = new GridFSDBFile();
        byte[] tem=new byte[1024];
        int len=0;

        while ((len=gridFSDownloadStream.read(tem)) !=-1) {
            fileOutputStream.write(tem, 0, len);
        };
        gridFSDownloadStream.close();
        fileOutputStream.close();

    }
    @Test
    public void testFridFsBucketFind(){
        MongoDatabase db = mongoDbFactory.getDb();

        GridFSBucket gridFSBucket = GridFSBuckets.create(db);
        Bson json = Filters.eq("metadata.wcontentType", "json");
        GridFSFindIterable gridFSFiles = gridFSBucket.find(json);
        for (GridFSFile gridFSFile :
        gridFSFiles) {
            gridFSBucket.openDownloadStream(gridFSFile.getObjectId());
            System.out.println(gridFSFile.getFilename());
        }

    }

    @Test
    public void testGridFs() throws IOException {
        MongoClient mongoClient = new MongoClient("172.17.3.191", 27017);
        DB db = new DB(mongoClient,"vfsTest");
        GridFS gridFS = new GridFS(db);
        File file = new File("");
        GridFSInputFile file1 = gridFS.createFile(file);

    }
    
    @Test
    public void testGridFsTemplate(){
        Document document = new Document();
        document.append("filename","高级复杂设计器json格式1.json");
        GridFSFindIterable gridFSFiles = gridFsTemplate.find(new BasicQuery(document));
        GridFsResource resource = gridFsTemplate.getResource("");
    }

    @Test
    public void testMongoTest(){
        DB db = new DB(mongoClient,"vfsTest");
        GridFS gridFS = new GridFS(db);
        GridFSDBFile gridFSDBFile = gridFS.findOne(new ObjectId("5ceb813e3436a44d744df48d"));
        Bson json = Filters.eq("metadata.wcontentType", "json");
        BasicDBObject basicDBObject = new BasicDBObject();
        basicDBObject.append("metadata.wcontentType","json");
        basicDBObject.append("metadata.wfilename","高级复杂设计器json格式1.json");
        GridFSDBFile one = gridFS.findOne(basicDBObject);
        System.out.println(one.getFilename());
    }
//    @Test
//    public void testMongoTest2() throws IOException {
//        GridFSDBFile one = gridFS.find(new ObjectId("5ceb813e3436a44d744df48d"));
//        InputStream inputStream = one.getInputStream();
//        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
//        String s="";
//        while ((s=bufferedReader.readLine()).equals("")){
//            System.out.println(s);
//        }
//    }

//    @Test
//    public void testUpdate() throws IOException {
//        File file = new File("C:\\Users\\wangyaofeng\\Desktop/高级复杂设计器json格式1.json");
//
//        GridFSUploadStream gridFSUploadStream = gridFSBucket.openUploadStream( "1111.json");
//
//        InputStream inputStream = new FileInputStream(file);
//        byte[] bytes = new byte[1024];
//        int n=0;
//        while ((n=inputStream.read(bytes))!=-1){
//            gridFSUploadStream.write(bytes,0,n);
//        }
//        inputStream.close();
//        gridFSUploadStream.close();
//
//    }
    @Test
    public void testTransaction(){
        MongoDatabase vfsTest = mongoClient.getDatabase("vfsTest");
        MongoCollection<Document> sdsd = vfsTest.getCollection("aaa");
        Document document = new Document();
        document.put("name","李白");
        com.mongodb.client.ClientSession clientSession = mongoClient.startSession();
        clientSession.startTransaction();
        sdsd.insertOne(clientSession,document);
        Document document1 = new Document();
        document1.put("name","李白");
        document1.put("age",18);
        sdsd.insertOne(clientSession,document1);
        clientSession.abortTransaction();
        clientSession.close();
    }

    @Test
    public void testManyUpdate(){
        MongoCollection<Document> fs = mongoDbFactory.getDb().getCollection("fs.files");
        FindIterable<Document> documents = fs.find(Filters.eq("filename", "aa.text"));
        Document first = documents.first();
        Document document = new Document();
        document.put("$set",new Document("metadata.name",true));
        fs.updateOne(Filters.eq("filename","aa.text"),document);

    }

}
