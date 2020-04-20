package com.wyf.vfs4mongo.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wyf.vfs.PropertyService;
import com.wyf.vfs4mongo.PropertyDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author wangyaofeng
 * @projectName vfs
 * @description:
 * @date 2019/6/27 17:28
 */
@Service
public class PropertyServiceImpl implements PropertyService {

    @Autowired
    PropertyDao propertyDao;
    @Autowired
    MongoDbFactory mongoDbFactory;
    @Autowired
    MongoTemplate mongoTemplate;

    @PostConstruct
    public void init(){
        boolean exists = mongoTemplate.collectionExists(VfsMongoProperty.class);
        if (!exists){
            IndexOperations indexOperations = mongoTemplate.indexOps(VfsMongoProperty.class);
            Index index = new Index();
            index.on("name", Sort.Direction.ASC );
            index.unique();
            indexOperations.ensureIndex(index);
        }
    }

    @Override
    public String getAll() {
        Sort name = Sort.by(Sort.Order.asc("name"));
        List<VfsMongoProperty> all = propertyDao.findAll(name);
        return JSONArray.toJSONString(all);
    }

    @Override
    public String getByName(String name) {
        VfsMongoProperty oneByName = propertyDao.findOneByName(name);
        if (oneByName==null){
            return null;
        }
        return JSONArray.toJSONString(oneByName);
    }

    @Override
    public boolean deleteByName(String name) {
        long l = propertyDao.deleteByName(name);
        return true;
    }

    @Override
    public boolean insertOne(JSONObject object) {
        VfsMongoProperty vfsMongoProperty = object.toJavaObject(VfsMongoProperty.class);
        propertyDao.save(vfsMongoProperty);
        return true;
    }

    @Override
    public boolean update(JSONObject object) {
        VfsMongoProperty vfsMongoProperty = object.toJavaObject(VfsMongoProperty.class);
        propertyDao.save(vfsMongoProperty);
        return true;
    }



}
