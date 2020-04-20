package com.wyf.vfs4mongo;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class BaseBean<T> {

    protected Map<String,Object> exts=new HashMap<>();

    protected String map="a";

    protected Date date;

    public Map<String, Object> getExts() {
        return exts;
    }

    public void setExts(Map<String, Object> exts) {
        this.exts = exts;
    }

    public void setMap(String map) {
        this.map = map;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Object getExtValue(String key){
        return exts.get(key);
    }

    public void setExtValue(String key, Object value) {
        exts.put(key,value);
//        org.bson.Document document = new org.bson.Document();
//        JSONObject jsonObject = JSONObject.parseObject(map);
//        if(jsonObject==null) jsonObject=new JSONObject();
//        jsonObject.put(key,value);
//        this.map=jsonObject.toJSONString();
    }

    public String getMap(){
        return this.map;
    }

}
