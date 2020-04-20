package com.wyf.vfs4mongo.impl;

import com.alibaba.fastjson.util.IOUtils;
import com.wyf.utils.BeansCont;
import com.wyf.utils.FileFunc;
import com.wyf.utils.SpringUtil;
import com.wyf.vfs.AbstractVfsFile;
import com.wyf.vfs.VfsException;
import com.wyf.vfs.VfsFile;
import com.wyf.vfs.impl.TransactionContext;
import com.wyf.vfs.impl.VfsMimeType;
import com.wyf.vfs.impl.VfsOutPutStream;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.result.DeleteResult;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component("vfs")
//@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@org.springframework.data.mongodb.core.mapping.Document("vfs")
@DependsOn({BeansCont.SPRINGUTIL})
public class VfsFile4Mongo extends AbstractVfsFile {

    private static final ThreadLocal<MongoTemplate> mongoTemplateThreadLocal = new ThreadLocal<>();


    private  static MongoTemplate mongoTemplate ;
    private  static GridFSBucket gridFSBucket ;
    private  static MongoClient mongoClient ;

    private static final int CHUNK_SIZE = 8*1024 *1024;

    public static  final Logger log = LoggerFactory.getLogger(VfsFile4Mongo.class);

    @Id
    private String id;

    private String fsId;


    /**
     * 父目录字段名
     */
    private static final String FIELD_PARENTDIR = "parentDir";
    /**
     * 文件名
     */
    private static final String FIELD_FILENAME = "name";
    /**
     * 是否是文件
     */
    private static final String FIELD_File = "file";

    @PostConstruct
    public void init(){

        mongoTemplate = SpringUtil.getApplicationContext().getBean(MongoTemplate.class);
        gridFSBucket = SpringUtil.getApplicationContext().getBean(GridFSBucket.class);;
        mongoClient = SpringUtil.getApplicationContext().getBean(MongoClient.class);
        //创建collection
        if(!mongoTemplate.collectionExists(VfsFile4Mongo.class)){
            mongoTemplate.createCollection(VfsFile4Mongo.class);

            createIndexes();

            createRoot();
        }
    }

    /**
     * 创建vfs所需索引
     */
    private void createIndexes(){
        IndexOperations indexOperations = mongoTemplate.indexOps(VfsFile4Mongo.class);
        //创建parentDir与name的复合索伊
        Document document = new Document();
        document.put(FIELD_PARENTDIR,1);
        document.put(FIELD_FILENAME,1);
        indexOperations.ensureIndex(new CompoundIndexDefinition(document).unique()) ;
        //创建索伊parentdir，name
        Index parentDirIndex = new Index();
        parentDirIndex.on(FIELD_PARENTDIR, Sort.Direction.ASC );
        Index nameIndex = new Index();
        nameIndex.on(FIELD_FILENAME, Sort.Direction.ASC);
        indexOperations.ensureIndex(parentDirIndex);
        indexOperations.ensureIndex(nameIndex);
    }

    /**
     * 创建根目录
     */
    private void createRoot(){
        //创建根目录
        VfsFile4Mongo vfsFile4Mongo = new VfsFile4Mongo();
        vfsFile4Mongo.setParentDir("");
        vfsFile4Mongo.setName("/");
        vfsFile4Mongo.setFile(false);
        vfsFile4Mongo.save();
    }

    @Override
    public VfsFile4Mongo getVfsFile(String filename) {

        String[] formats = FileFunc.sepPath(filename);
        return this.getVfsFile(formats[0],formats[1]);
    }
    @Override
    public VfsFile4Mongo getRoot() {
        return this.getVfsFile("/","/");
    }

    /**
     * 根据目录，跟文件名获取文件
     * @param parentDir
     * @param filename
     */
    @Override
    public VfsFile4Mongo getVfsFile(String parentDir,String filename){
        VfsFile4Mongo vfsFile = null;
        try {
            vfsFile = findVfsFile(parentDir, filename);
        } catch (BeansException e) {
            new VfsException(e.getMessage());
        }
        if (vfsFile==null){
            vfsFile = new VfsFile4Mongo();
            //设置文件的目录路径跟名称
            vfsFile.setParentDir(parentDir);
            vfsFile.setName(filename);
        }else{
            //检查权限
            vfsFile.exist=true;
            this.checkCanRead(vfsFile);
        }
        return vfsFile;
    }

    /**
     * 获取VfsFile4Mongo
     * @param parentDir
     * @param filename
     * @return
     */
    private VfsFile4Mongo findVfsFile(String parentDir,String filename){
        //拼接查询条件
        Document document = new Document(FIELD_PARENTDIR, parentDir);
        document.put(FIELD_FILENAME,filename);
        BasicQuery query = new BasicQuery(document);
        VfsFile4Mongo one = mongoTemplate.findOne(query, VfsFile4Mongo.class);
        return one;
    }

    /**
     * 获取文件的子文件
     * @param vfsFile
     * @param keyword
     * @param recur
     * @return
     */
        private VfsFile4Mongo[] findChilds(VfsFile4Mongo vfsFile,String keyword,boolean recur){
        checkCanRead(vfsFile);
        if (vfsFile.isFile()){
            throw new VfsException("此文件不是目录，不能进行此操作！");
        }
        String parentDir = vfsFile.getAbsolutePath().equals(FileFunc.separator)?vfsFile.getAbsolutePath():vfsFile.getAbsolutePath()+FileFunc.separator;
        BasicQuery query = new BasicQuery(new Document());
        //拼接查询条件
        buildFindChildsQuery(query,parentDir,keyword,recur);
        List<VfsFile4Mongo> vfsFile4Mongos = mongoTemplate.find(query, VfsFile4Mongo.class);
        if(vfsFile4Mongos==null || vfsFile4Mongos.size()==0){
            return new VfsFile4Mongo[0];
        }
        for (VfsFile4Mongo vfsFile4Mongo : vfsFile4Mongos) {
            vfsFile4Mongo.exist = true;
        }
        Collections.sort(vfsFile4Mongos);
        return  vfsFile4Mongos.toArray(new VfsFile4Mongo[vfsFile4Mongos.size()]);
    }

    /**
     *  拼接查询条件
     * @param query
     * @param parentDir
     * @param keyword
     * @param recur
     */
    private void buildFindChildsQuery(BasicQuery query, String parentDir, String keyword, boolean recur) {
        if (recur){
            query.addCriteria(Criteria.where(FIELD_PARENTDIR).regex(new StringBuilder(20).append("^").append(parentDir).append(".*$").toString()));
        }else{
            query.addCriteria(Criteria.where(FIELD_PARENTDIR).is(parentDir));
        }
        if (!Strings.isNullOrEmpty(keyword)){
            query.addCriteria(Criteria.where(FIELD_FILENAME).regex( keyword));
        }
    }
//    /**
//     * 把查询出的文档数据封装到vfsfile实例里面
//     * @param vfsFile
//     */
//    public void setVfsFile(VfsFile4Mongo vfsFile ){
//        if(this.gridFSDBFile==null){
//            return;
//        }
//        vfsFile.set_id((ObjectId) this.gridFSDBFile.getId());
//        vfsFile.setName(this.gridFSDBFile.getFilename());
//        vfsFile.setModifyTime(this.gridFSDBFile.getUploadDate());
//        vfsFile.setSize((int)this.gridFSDBFile.getLength());
//        DBObject metaData = this.gridFSDBFile.getMetaData();
//        for (String key : metaData.keySet()) {
//            switch (key){
//                case "parentDir" :  vfsFile.setParentDir((String) metaData.get(key));break;
//                case "file" :  vfsFile.setFile((Boolean) metaData.get(key));break;
//                case "createTime" :  vfsFile.setCreateTime((Date) metaData.get(key));break;
//                case "updateTime" :  vfsFile.setModifyTime((Date) metaData.get(key));break;
//                case "owner" :  vfsFile.setOwner((String) metaData.get(key));break;
//                case "mender" :  vfsFile.setMender((String) metaData.get(key));break;
//                case "charset" : vfsFile.setCharset((String) metaData.get(key)); break;
//                case "mineType" : vfsFile.setMineType((String) metaData.get(key)); break;
//                default: vfsFile.setExtValue(key,metaData.get(key));
//            }
//        }
//        vfsFile.exist=true;
//    }

    @Override
    public String getFileAsString(String filename) throws VfsException {
        return this.getVfsFile(filename).getContentAsString();
    }

    @Override
    public void setFileAsString(String filename, String content) throws VfsException {
        try {
            byte[] bytes = content.getBytes(this.getCharset());
            this.setFileAsBytes(filename,bytes);
        } catch (UnsupportedEncodingException e) {
            throw new VfsException(e.getMessage());
        }
    }

    @Override
    public byte[] getFileAsBytes(String filename) throws VfsException {
        return  this.getVfsFile(filename).getContentAsBytes();
    }



    /**
     * 保存vfsfile
     * @param vfsFile
     */
    public void save(VfsFile4Mongo vfsFile){
        checkCanWrite(vfsFile);
        //如果之前，带开了事务，返回true
        boolean openTransaction = beforeTransaction();

        try {
            //如果父文件夹名称不为"/"，则确认父文件夹是否存在，不存在则创建
            if(!exist && !Strings.isNullOrEmpty(vfsFile.getParentDir()) && !vfsFile.getParentDir().equals(FileFunc.separator)){
                ensureParentExist(vfsFile.getParent());
            }

            if (vfsFile.isExist()){
                createVfsFile(vfsFile);
            }else {
                //如果文件在mongodb中不存在，且事务期间也没创建过，则执行，否则不创建vfsfile
                if (!PathsThreadLocal.containsPath(vfsFile.getAbsolutePath())){
                    createVfsFile(vfsFile);
                    //去掉事务后，必须要注释掉此段代码
//                    PathsThreadLocal.setPath(vfsFile.getAbsolutePath());
                }
            }
            afterTransactionOfCommit(openTransaction);
        } finally {
            afterTransactionOfEnd(openTransaction);
        }



    }

    /**
     * 确保父目录存在，如果不存在则创建
     * @param parent
     */
    private void ensureParentExist(String parent) {
        VfsFile4Mongo vfsFile = this.getVfsFile(parent);
        if(!vfsFile.isExist()){
            vfsFile.mkdirs();
        }
    }

    /**
     * 保存自身
     */
    public void save(){
        this.save(this);
    }

    /**
     * 把vfsfile的属性保存到gridfsdbfile中
     * @param vfsFile
     */
//    private void setGridFileProp(VfsFile4Mongo vfsFile) {
//        checkArguments(vfsFile);
//
//        if(vfsFile.getGridFSDBFile()==null) return;
//        if(vfsFile.getCreateTime()==null) vfsFile.setCreateTime(new Date());
//        vfsFile.setModifyTime(new Date());
//        if(Strings.isNullOrEmpty(vfsFile.getOwner())) vfsFile.setOwner("admin");
//        if(Strings.isNullOrEmpty(vfsFile.getMender())) vfsFile.setMender("admin");
//
//        vfsFile.getGridFSDBFile().put("_id", vfsFile.get_id());
//        vfsFile.getGridFSDBFile().put("filename", vfsFile.getName());
//        vfsFile.getGridFSDBFile().put("length", vfsFile.getSize());
//        vfsFile.getGridFSDBFile().put("uploadDate", vfsFile.getCreateTime());
//
//        BasicDBObject document = new BasicDBObject();
//        document.append("parentDir",vfsFile.getParentDir());
//        document.append("file",vfsFile.isFile());
//        document.append("createTime",vfsFile.getCreateTime());
//        document.append("updateTime",vfsFile.getModifyTime());
//        document.append("owner",vfsFile.getOwner());
//        document.append("mender",vfsFile.getMender());
//        document.append("charset",vfsFile.getCharset());
//        document.append("mineType",vfsFile.getMineType());
//        for ( String key :
//                vfsFile.getAllExtValue().keySet()) {
//            document.append(key,vfsFile.getExtValue(key));
//        }
//        vfsFile.getGridFSDBFile().setMetaData(document);
//
//    }


    /**
     * 创建vfsfile
     * @param vfsFile
     */
    private void createVfsFile(VfsFile4Mongo vfsFile){

        try {
            checkArguments(vfsFile);

            checkFieldsBeforeSave(vfsFile);

            vfsFile.exist=true;

            MongoTemplate mongoTemplate = getMongoTemplate();
            mongoTemplate.save(vfsFile);

            //如果是目录或大小为0，不上传文件
            if(vfsFile.isDir() || vfsFile.getSize()==0){
                return;
            }
            GridFSUploadOptions options = new GridFSUploadOptions()
                    .chunkSizeBytes(CHUNK_SIZE);
            this.gridFSBucketOfUploadFromStream(new BsonObjectId(new ObjectId(vfsFile.getFsId())),vfsFile.getName(), new ByteArrayInputStream(vfsFile.getContent()), options);
        } catch (VfsException e) {
            vfsFile.exist=false;
            throw new VfsException(e);
        }

    }

    /**
     * 检查vfsfile及其属性
     * @param vfsFile
     */
    private void checkArguments(VfsFile4Mongo vfsFile) {
        if(vfsFile==null){
            throw new NullPointerException("参数vfsfile不能为空!");
        }
        if(Strings.isNullOrEmpty(vfsFile.getName())){
            throw new IllegalArgumentException("name属性不能为空!");
        }
        if(Strings.isNullOrEmpty(vfsFile.getParentDir() ) && !vfsFile.getName().equals(FileFunc.separator)){
            throw new IllegalArgumentException("parentDir属性不能为空!");
        }
        if( !Strings.isNullOrEmpty(vfsFile.getParentDir() ) && vfsFile.getName().equals(FileFunc.separator)){
            throw new IllegalArgumentException("文件名不能为 \"/\" !");
        }
    }

    /**
     *  保存前，检查属性
     * @param vfsFile
     */
    private void checkFieldsBeforeSave(VfsFile4Mongo vfsFile) {
        //保存前，设置属性
        if(vfsFile.getCreateTime()==null){
            vfsFile.setCreateTime(new Date());
        }
        vfsFile.setModifyTime(new Date());
        if(Strings.isNullOrEmpty(vfsFile.getOwner())){
            vfsFile.setOwner("admin");
        }
        if(Strings.isNullOrEmpty(vfsFile.getMender())){
            vfsFile.setMender("admin");
        }
        vfsFile.setMimeType(VfsMimeType.getContentType(this.name));
        if(vfsFile.isDir() || vfsFile.getSize()==0){
            return;
        }else{
            vfsFile.getContent();  //防止内容没有被加载到内存中，在删掉原来的数据后，保存时报错
            if (vfsFile.isExist() && !Strings.isNullOrEmpty(vfsFile.getFsId())){
                this.gridFSBucketOfDelete(new ObjectId(vfsFile.getFsId()));
            }
        }
        vfsFile.setFsId(new ObjectId().toString());
    }

    @Override
    public void setFileAsBytes(String filename, byte[] buf) throws VfsException {
        VfsFile4Mongo vfsFile = this.getVfsFile(filename);
        vfsFile.setAsBytes(buf);
    }

    @Override
    public void removeFile(String filename) throws VfsException {
        this.getVfsFile(filename).delete();
    }



    @Override
    public String getContentAsString() throws VfsException {
        byte[] content = this.getContentAsBytes();
        if(content !=null){
            String s = null;
            try {
                s = new String(content, this.getCharset());
            } catch (UnsupportedEncodingException e) {
                throw new VfsException(e.getMessage());
            }
            return s;
        }
        return null;
    }

    @Override
    public byte[] getContentAsBytes() throws VfsException {
        if(!this.isExist() || this.isDir()){
            return null;
        }
        if(this.getSize()==0){
            return new byte[0];
        }
        return this.getContent();
    }

    @Override
    public OutputStream getOutPutStream() throws VfsException {
        return new VfsOutPutStream(this);
    }

    @Override
    public InputStream getInputStream() throws VfsException {
        checkCanRead(this);
        if(!this.isExist()){
            throw new VfsException("文件不存在");
        }
        if(this.isDir()){
            throw new VfsException("目录不能进行此操作！");
        }
        return new ByteArrayInputStream(this.getContent());
    }

    @Override
    public void saveAsString(String v, String charset) throws VfsException {
        if(v==null || Strings.isNullOrEmpty(charset)){
            throw new IllegalArgumentException("参数不能为空!");
        }
        try {
            this.setCharset(charset);
            this.setAsBytes(v.getBytes(charset));
        } catch (UnsupportedEncodingException e) {
            throw new VfsException(e.getMessage());
        }
    }

    @Override
    public void setAsBytes(byte[] bytes) throws VfsException {
        String encoding = FileFunc.getEncoding(bytes);
        this.setCharset(encoding);
        this.setContent(bytes);
        this.save();
    }

    @Override
    public void setAsBytesNoTransaction(byte[] bytes) throws VfsException {
        String encoding = FileFunc.getEncoding(bytes);
        this.setCharset(encoding);
        this.setContent(bytes);
        this.createVfsFile(this);
    }

    @Override
    public void saveAsString(String v) throws VfsException {
        this.saveAsString(v,this.getCharset());
    }


    @Override
    public VfsFile[] getChilds() throws VfsException {
        return findChilds(this,null,false);
    }


    @Override
    public VfsFile getParentFile() throws VfsException {
        String parentPath = this.getParent();
        if(parentPath==null){
            return null;
        }
        return this.getVfsFile(parentPath);
    }

    @Override
    public boolean renameTo(String fnOrDir) throws VfsException {
        return renameTo(fnOrDir,false);
    }

    @Override
    public boolean renameTo(String fnOrDir, boolean isoverwrite) throws VfsException {
        if(Strings.isNullOrEmpty(fnOrDir)) {
            throw new VfsException("名称不能为空!");
        }
        checkCanWrite(this);
        //检查名称是否合法
        FileFunc.checkValidFileName(fnOrDir);
        //fnOrDir = FileFunc.replaceSeparatorChar(fnOrDir);
        String newParentDir=this.getParentDir();
        String newName=fnOrDir;
        if(this.getName().equals(newName)){
            return true;
        }
        VfsFile4Mongo destFile = this.getVfsFile(newParentDir + newName);
        //检查当前文件下是否存在同名文件
        if(!isoverwrite && destFile.isExist()){
            throw new VfsException("当前目录下已经存在同名文件！");
        }
        //确保父目录存在
        ensureParentExist(destFile.getParent());

        _renameTo(this,destFile,isoverwrite);

        return true;
    }

    /**
     * f1是否是f2的上级目录
     * @param f1
     * @param f2
     * @return
     */
    private boolean isParentFile(VfsFile4Mongo f1,VfsFile4Mongo f2){
        if(f1.isExist() && f1.isDir()){
            String parent = f2.getParent();
            while (parent!=null ){
                if(f1.getAbsolutePath().equals(parent)){
                    return true;
                }
                parent = this.getParent(parent);
            }
        }
        return false;
    }

    private String getParent(String path){
        String parentDir = FileFunc.sepPath(path)[0];
        return getParentByParentDir(parentDir);
    }


    /**
     * 将src重命名为dest
     * @param src
     * @param dest
     * @param isoverwrite
     */
    private void _renameTo(VfsFile4Mongo src, VfsFile4Mongo dest, boolean isoverwrite) {
        if(src.getAbsolutePath().equals(dest.getAbsolutePath())){
            return ;
        }
        boolean transaction = true;
        if (src.isDir()){
             transaction = beforeTransaction();
        }
        try {
            MongoTemplate mongoTemplate = getMongoTemplate();
            String newPath=dest.getParentDir();
            String name=dest.getName();
            String oldAbPath=src.getAbsolutePath()+"/";
            src.setParentDir(dest.getParentDir());
            src.setName(dest.getName());
            src.save();

            //如果是目录，则需要更改下级文件的parentdir
            if(src.isDir()){
                String newAbPath=src.getAbsolutePath()+"/";
                BasicQuery basicQuery = new BasicQuery(new Document());
                basicQuery.addCriteria(Criteria.where(FIELD_PARENTDIR).regex(Pattern.compile("^" + oldAbPath + ".*$")));

                List<VfsFile4Mongo> vfsFile4Mongos = mongoTemplate.find(basicQuery, VfsFile4Mongo.class);
                if(vfsFile4Mongos.size()!=0){
                    BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, VfsFile4Mongo.class);
                    Consumer<? super VfsFile4Mongo> action = x -> {
                        String oldParentDir = x.getParentDir();
                        BasicQuery query = new BasicQuery(new Document());
                        query.addCriteria(Criteria.where(FIELD_PARENTDIR).is(oldParentDir));
                        query.addCriteria(Criteria.where(FIELD_FILENAME).is(x.getName()));

                        String newParentDir = oldParentDir.replaceFirst(oldAbPath, newAbPath);

                        BasicUpdate update = new BasicUpdate(new Document("$set",new Document(FIELD_PARENTDIR,newParentDir)));

                        bulkOperations.updateOne(query,update);
                    };
                    vfsFile4Mongos.forEach(action);
                    bulkOperations.execute();
                }
                afterTransactionOfCommit(transaction);
            }
        } finally {
            if (src.isDir()) {
                afterTransactionOfEnd(transaction);
            }
        }

    }

    @Override
    public boolean moveTo(VfsFile destDir) throws VfsException {
        checkCanWrite(this);
        VfsFile4Mongo vf= (VfsFile4Mongo) destDir;
        if(!vf.isExist()){
            throw new VfsException(vf.getAbsolutePath()+"不存在！");
        }
        if(this.getAbsolutePath().equals(vf.getAbsolutePath())){
            throw new VfsException("移动目地文件夹不能是自己！");
        }
        if(isParentFile(this,vf)){
            throw new VfsException("不可移动到子目录下！");
        }

        VfsFile4Mongo vfsFile = this.getVfsFile(vf.getAbsolutePath()+"/"+ this.getName());
        if(vfsFile.isExist()){
            throw new VfsException(vf.getAbsolutePath()+"文件夹下存在同名文件！");
        }

        _renameTo(this,vfsFile,true);
        return  true;
    }

    @Override
    public boolean copyTo(VfsFile destDir, String name, boolean isoverwrite) throws VfsException {
        return false;
    }

    @Override
    public boolean isExist() throws VfsException {
        return this.exist;
    }



    @Override
    public void create() throws VfsException {
        this.save();
    }

    @Override
    public void mkdirs() throws VfsException {
        this.setFile(false);
        this.setContent(new byte[0]);
        this.create();
    }

    @Override
    public void delete() throws VfsException {
        checkCanRemove(this);
        boolean openTransaction = this.beforeTransaction();

        try
        {
            MongoTemplate mongoTemplate = this.getMongoTemplate();
            DeleteResult remove = mongoTemplate.remove(this);
            if(this.isDir()){
                this.removeChilds(this.getAbsolutePath());
            }else {
                if (this.length()!=0){
                    this.gridFSBucketOfDelete(new ObjectId(this.getFsId()));
                }
            }
            afterTransactionOfCommit(openTransaction);
        } finally {
            this.afterTransactionOfEnd(openTransaction);
        }
    }

    /**
     * 删除目录的所有下级，不包括自身
     * @param path
     */
    private void removeChilds(String path){
        boolean b = beforeTransaction();
        try {
            MongoTemplate mongoTemplate = this.getMongoTemplate();
            List<VfsFile4Mongo> list = mongoTemplate.findAllAndRemove(new BasicQuery(new Document()).addCriteria(Criteria.where(FIELD_PARENTDIR).regex(Pattern.compile("^" + path + ".*$"))), VfsFile4Mongo.class);
            for (VfsFile4Mongo vfsFile4Mongo :
                    list) {
                if (vfsFile4Mongo.isExist() && vfsFile4Mongo.isFile()){
                    this.gridFSBucketOfDelete(new ObjectId(vfsFile4Mongo.getFsId()));
                }
            }
            afterTransactionOfCommit(b);
        } finally {
            afterTransactionOfEnd(b);
        }
    }

    @Override
    public void importFile(File file) throws VfsException {
        importFile(file,false);
    }

    private void importFileNotCheck(File file,String dir,boolean deleteFirst) throws VfsException{
        boolean b = beforeTransaction();
        try {
            if (!file.exists()){
                throw new FileNotFoundException("文件不存在");
            }
            if (Strings.isNullOrEmpty(dir)){
                dir = "/";
            }
            VfsFile4Mongo vfsFile = this.getVfsFile(this.getAbsolutePath()+ dir, file.getName());
            if (file.isDirectory()){
                if (vfsFile.isExist()) {
                    new VfsException("文件重名"+file.getName());
                }
                for (File child : file.listFiles()) {
                    importFileNotCheck(child,dir+file.getName()+"/",deleteFirst);
                }
            }else{
                byte[] bytes = Files.toByteArray(file);
                if (vfsFile.isExist() && !deleteFirst){
                    throw new VfsException("文件重名");
                }
                if (vfsFile.isExist()){
                    vfsFile.delete();
                }
                vfsFile.setAsBytes(bytes);
            }
            afterTransactionOfCommit(b);
        } catch (IOException e) {
            throw  new VfsException(e);
        }finally {
            afterTransactionOfEnd(b);
        }
    }

    @Override
    public void importFile(File file, boolean deleteFirst) throws VfsException {
        if (!this.isExist()){
            this.mkdirs();
        }
        if (this.isFile()){
            throw new VfsException("只有目录才能进行此操作！");
        }
        checkCanCreate(this);
        importFileNotCheck(file,"",deleteFirst);
    }

    @Override
    public void importStm(InputStream in, String name) throws VfsException {
        if (!this.isExist()){
            this.mkdirs();
        }
        checkCanCreate(this);
        try {
            byte[] bytes = ByteStreams.toByteArray(in);
            this.setFileAsBytes(this.getAbsolutePath()+"/"+name,bytes);
        } catch (IOException e) {
            throw new VfsException(e);
        }
    }

    @Override
    public void importZip(InputStream in) throws VfsException {
        if (!this.isExist()){
            this.mkdirs();
        }
        if (this.isFile()){
            throw new VfsException("只有目录才能进行此操作！");
        }
        checkCanCreate(this);
        ZipInputStream zipis = null;
        ZipEntry zipEntry=null;
        //boolean b = beforeTransaction();
        try {
            zipis = new ZipInputStream(in);
            while((zipEntry= zipis.getNextEntry())!=null){
                if (!zipEntry.isDirectory()){
                    VfsFile4Mongo vfsFile = this.getVfsFile(this.getAbsolutePath()+"/"+zipEntry.getName());
                    if (!vfsFile.isExist()){
                        byte[] bytes = ByteStreams.toByteArray(zipis);
                        vfsFile.setAsBytes(bytes);
                        bytes=null;
                    }
                }
                zipis.closeEntry();
            };
           // afterTransactionOfCommit(b);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (zipis!=null){
                IOUtils.close(zipis);
            }
            //afterTransactionOfEnd(b);
        }
    }


    @Override
    public void writeContentTo(OutputStream out, boolean zip) throws VfsException {
        checkCanRead(this);
        byte[] content = this.getContent();
        try {
            out.write(content,0,content.length);
        } catch (IOException e) {
            throw new VfsException(e);
        }
    }

    @Override
    public VfsFile[] listFiles(String filterwildcard, int filterType, boolean recur) throws VfsException {


        checkCanRead(this);
        if (this.isFile()){
            throw new VfsException("此文件不是目录，不能进行此操作！");
        }
        String parentDir = this.getAbsolutePath().equals(FileFunc.separator)?this.getAbsolutePath():this.getAbsolutePath()+FileFunc.separator;
        BasicQuery query = new BasicQuery(new Document());
        //拼接查询条件
        buildQuery(query,parentDir,filterwildcard,recur,filterType);
        List<VfsFile4Mongo> vfsFile4Mongos = mongoTemplate.find(query, VfsFile4Mongo.class);
        if(vfsFile4Mongos==null || vfsFile4Mongos.size()==0){
            return new VfsFile4Mongo[0];
        }
        for (VfsFile4Mongo vfsFile4Mongo : vfsFile4Mongos) {
            vfsFile4Mongo.exist = true;
        }
        Collections.sort(vfsFile4Mongos);
        return  vfsFile4Mongos.toArray(new VfsFile4Mongo[vfsFile4Mongos.size()]);
    }

    /**
     * listFile 拼接查询条件
     * @param query
     * @param parentDir
     * @param filterwildcard
     * @param recur
     * @param filterType
     */
    private void buildQuery(BasicQuery query, String parentDir, String filterwildcard, boolean recur, int filterType) {
        boolean filterFile = (VfsFile.FILTERFILE & filterType) != 0;
        boolean filterFolder = (VfsFile.FILTERFOLDER & filterType) != 0;
        boolean reserveFile = (VfsFile.RESERVEFILE & filterType) != 0;
        boolean reserveFolder = (VfsFile.RESERVEFOLDER & filterType) != 0;

        if (recur){
            query.addCriteria(Criteria.where(FIELD_PARENTDIR).regex(new StringBuilder(20).append("^").append(parentDir).append(".*$").toString()));
        }else{
            query.addCriteria(Criteria.where(FIELD_PARENTDIR).is(parentDir));
        }
        if (!Strings.isNullOrEmpty(filterwildcard)){
            filterwildcard=filterwildcard.replaceAll("[.*]",".*");
            filterwildcard=filterwildcard.replaceAll("[?]",".");
        }
        Criteria fileCriteria = getCriteriaByFilterAndReserve(filterwildcard, filterFile, reserveFile, true);
        Criteria folerCriteria = getCriteriaByFilterAndReserve(filterwildcard, filterFolder, reserveFile, false);
        if(fileCriteria!=null&&folerCriteria!=null){
            Criteria criteria = new Criteria();
            query.addCriteria(criteria.orOperator(fileCriteria,folerCriteria));

        }else{
            if(fileCriteria!=null){
                query.addCriteria(fileCriteria);
            }
            if(folerCriteria!=null){
                query.addCriteria(folerCriteria);
            }
        }
    }


    private Criteria getCriteriaByFilterAndReserve(String filterwildcard,boolean filter,boolean reserve,boolean file){
        Criteria critetia = null;
        if(filter&&reserve){
            critetia = getCriteria(filterwildcard,file);
        }else{
            if (filter){
                critetia =getCriteria(filterwildcard,file);
            }
            if (reserve){
                critetia = Criteria.where(FIELD_File).is(true);
            }
        }
        return critetia;
    }


    private Criteria getCriteria(String filterwildcard,boolean isFile){

        if(!Strings.isNullOrEmpty(filterwildcard)){
            Criteria critetia = new Criteria();
            return critetia.andOperator(Criteria.where(FIELD_FILENAME).regex( new StringBuilder(20).append("^").append(filterwildcard).append("$").toString()),Criteria.where(FIELD_File).is(isFile));
        }else{
            return Criteria.where(FIELD_File).is(isFile);
        }
    }



    @Override
    public VfsFile[] listFiles(String keyword, boolean recur) throws VfsException {
        return findChilds(this,keyword,recur);
    }



    @Override
    public void startTransaction() throws VfsException {
//        Object session = TransactionContext.currentSession();
//        if(session==null){
//            session = mongoClient.startSession();
//            TransactionContext.setCurrentSession(session);
//            ((ClientSession)session).startTransaction();
//        }
    }

    /**
     * 判断是否已经打开事务
     * @return
     */
    private boolean isOpenTransaction(){
        Object session = TransactionContext.currentSession();
        if(session == null){
            return false;
        }
        return true;
    }

    @Override
    public void commitTransaction() throws VfsException {
//        Object session = TransactionContext.currentSession();
//        if(session == null){
//            throw new VfsException("尚未开启事务！");
//        }
//        ((ClientSession)session).commitTransaction();
//        PathsThreadLocal.clearAll();
    }

    @Override
    public void endTransaction() throws VfsException {
//        Object session = TransactionContext.currentSession();
//        if(session == null){
//            throw new VfsException("尚未开启事务！");
//        }
//        ((ClientSession)session).close();
//        TransactionContext.setCurrentSession(null);
//        mongoTemplateThreadLocal.set(null);
//        PathsThreadLocal.clearAll();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getFsId() {
        return fsId;
    }

    public void setFsId(String fsId) {
        this.fsId = fsId;
    }

    @Override
    public byte[] getContent() {
        if(this.isDir()){
            return null;
        }
        if(super.getContent()!=null){
            return (byte[]) super.getContent();
        }
        if(super.getSize()==0){
            this.setContent(new byte[0]);
            return (byte[]) super.getContent();
        }
        GridFSDownloadStream inputStream = null;

        try {
            ClientSession clientSession = getClientSession();
            inputStream = this.gridFSBucketOfOpenDownloadStream(new ObjectId(this.getFsId()));
            byte[] bytes = new byte[this.getSize()];
            int i = inputStream.read(bytes);
            this.setContent(bytes);
        }finally {
            if (inputStream!=null) {
                inputStream.close();
            }
        }
        return (byte[]) super.getContent();

    }

    @Override
    public void setContent(Object content) {
        if(content==null){
            this.setSize(0);
        }else{
            byte[] current= (byte[]) content;
            this.setSize(current.length);
        }
        super.setContent(content);
    }

    /**
     *  从线程副本中获取session
     * @return
     */
    private ClientSession getClientSession(){
        Object session = TransactionContext.currentSession();
        return session==null ? null : (ClientSession)session;
    }

    /**
     * 获取mongoTemplate
     * @return
     */
    private MongoTemplate getMongoTemplate(){
        MongoTemplate mongoTemplate = mongoTemplateThreadLocal.get();
        if (mongoTemplate==null ){
            Object session = TransactionContext.currentSession();
            if (session == null){
                return VfsFile4Mongo.mongoTemplate;
            }else{
                mongoTemplate= VfsFile4Mongo.mongoTemplate.withSession((ClientSession) session);
                mongoTemplateThreadLocal.set(mongoTemplate);
            }
        }
        return mongoTemplate;
    }

    /**
     *  内部开启事务调用的方法
     *  返回是否之前已经打开事务
     * @return
     */
    private boolean beforeTransaction(){
        boolean openTransaction = isOpenTransaction();
        if(!openTransaction){
            this.startTransaction();
        }
        return openTransaction;
    }

    /**
     *  内部提交事务调用
     * @param openTransaction
     */
    private void afterTransactionOfCommit(boolean openTransaction){
        if(!openTransaction){
            this.commitTransaction();
        }
    }

    /**
     *  内部结束事务调用的方法
     * @param openTransaction
     */
    private void afterTransactionOfEnd(boolean openTransaction){
        if(!openTransaction){
            this.endTransaction();
        }
    }

    /**
     *  gridfsbucket 删除
     * @param id
     */
    public void gridFSBucketOfDelete(ObjectId id){
        ClientSession clientSession = getClientSession();
        if(clientSession==null){
            gridFSBucket.delete(id);
        }else{
            gridFSBucket.delete(clientSession,id);
        }
    }

    /**
     *   gridfsbucket 打开下载流
     * @param id
     * @return
     */
    public GridFSDownloadStream gridFSBucketOfOpenDownloadStream(ObjectId id){
        ClientSession clientSession = getClientSession();
        GridFSDownloadStream gridFSDownloadStream = null;
        if(clientSession==null){
            gridFSDownloadStream = gridFSBucket.openDownloadStream(id);
        }else{
            gridFSDownloadStream = gridFSBucket.openDownloadStream(clientSession,id);
        }
        return gridFSDownloadStream;
    }

    /**
     *  gridfsbucket 上传流
     * @param id
     * @param filename
     * @param source
     * @param options
     */
    public void gridFSBucketOfUploadFromStream(BsonValue id,String filename,InputStream source,GridFSUploadOptions options){
        ClientSession clientSession = getClientSession();
        if(clientSession==null){
            gridFSBucket.uploadFromStream(id,filename,source,options);
        }else{
            gridFSBucket.uploadFromStream(clientSession,id,filename,source,options);
        }
    }

    /**
     *  gridfsbucket 查找
     * @param bson
     * @return
     */
    public GridFSFindIterable gridFSBucketOfFind(Bson bson){
        ClientSession clientSession = getClientSession();
        GridFSFindIterable gridFSFiles = null;
        if(clientSession==null){
            gridFSFiles = gridFSBucket.find(bson);
        }else{
            gridFSFiles = gridFSBucket.find(clientSession,bson);
        }
        return gridFSFiles;
    }

    @Override
    public boolean isHidden() throws VfsException {
        return false;
    }

    public void setAsBytes(){

    }

}
