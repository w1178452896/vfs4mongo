package com.wyf.vfs4mongo.impl;

import com.wyf.vfs.impl.VfsExtendPropertyBean;
import com.wyf.vfs4mongo.VfsCons4Mongo;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author wangyaofeng
 * @projectName vfs
 * @description:
 * @date 2019/6/27 16:18
 */
@Document(VfsCons4Mongo.EXT_PROPERTY_COLNAME)
public class VfsMongoProperty extends VfsExtendPropertyBean {

    @Id
    String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
