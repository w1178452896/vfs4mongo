package com.wyf.vfs4mongo;

import com.wyf.vfs4mongo.impl.VfsMongoProperty;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

/**
 * @author wangyaofeng
 * @projectName vfs
 * @description:
 * @date 2019/6/27 17:23
 */
@Repository
public interface PropertyDao extends MongoRepository<VfsMongoProperty,String> {

    public long deleteByName(String name);

    public VfsMongoProperty findOneByName(String name);


}
