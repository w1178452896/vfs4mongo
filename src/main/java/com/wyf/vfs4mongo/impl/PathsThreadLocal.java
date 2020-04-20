package com.wyf.vfs4mongo.impl;

import com.wyf.vfs.VfsException;
import com.google.common.base.Strings;
import org.springframework.core.NamedThreadLocal;
import org.springframework.util.Assert;

import java.util.HashSet;
import java.util.Set;

/**
 * @author wangyaofeng
 * @projectName vfs
 * @description:  存放当前线程创建的文件
 * @date 2019/6/21 16:07
 */
public class PathsThreadLocal {

    private static final ThreadLocal<Set<String>> pathThreadLocal = new NamedThreadLocal<>("pathsThreadLoacl");


    /**
     * 如何包含，直接报错
     */
    public static boolean containsPath(String path){
        if ( Strings.isNullOrEmpty(path)){
            Assert.notNull(path,"path不能为空!");
        }
        Set<String> set = pathThreadLocal.get();
        if (set==null){
            set = new HashSet<String>();
            pathThreadLocal.set(set);
        }
        return set.contains(path);
    }

    /**
     * 把创建成功的vfsFile 的路径放入threadlocal
     * 如何已经存在此文件，直接报错
     * @param path
     */
    public static void setPath(String path){
        boolean b = containsPath(path);
        if (b){
            throw new VfsException("当前事务中已经创建了："+path);
        }
        Set<String> set = pathThreadLocal.get();
        set.add(path);
    }

    public static void clearAll(){
        pathThreadLocal.set(null);
    }



}
