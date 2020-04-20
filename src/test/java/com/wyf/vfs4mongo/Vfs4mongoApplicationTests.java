package com.wyf.vfs4mongo;

import com.wyf.vfs.VfsFactory;
import com.wyf.vfs.VfsFile;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.w3c.dom.Document;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

@RunWith(SpringRunner.class)
@SpringBootTest
public class Vfs4mongoApplicationTests {



    @Test
    public void contextLoads() {
        VfsFile vfs= VfsFactory.getVfs();
        VfsFile vfsFile = vfs.getVfsFile("/import");
        File file = new File("C:\\Users\\wangyaofeng\\Desktop/阿里巴巴Java开发手册终极版v1.3.0.pdf");
        vfsFile.importFile(file,true);
    }
    @Test
    public void contextLoads2() {
        VfsFile vfs= VfsFactory.getVfs();
        VfsFile vfsFile = vfs.getVfsFile("/import/aaa");
        File file = new File("C:\\Users\\wangyaofeng\\Desktop/主题集赤峰属地端等备份_20190530_102151.zip");
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            vfsFile.importZip(fileInputStream);
            fileInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void contextLoads33() {
        VfsFile vfs= VfsFactory.getVfs();
        VfsFile vfsFile1 = vfs.getVfsFile("/");
        VfsFile[] vfsFiles = vfsFile1.listFiles("bb**", VfsFile.FILTERFILE+VfsFile.FILTERFOLDER, true);
    }
    @Test
    public void contextLoads3() {
        VfsFile vfs= VfsFactory.getVfs();
        VfsFile vfsFile = vfs.getVfsFile("/root/backup.xml");
        Document contentAsXml = vfsFile.getContentAsXml();
        VfsFile vfsFile1 = vfs.getVfsFile("/import/aaa/backup.xml");
        vfsFile1.saveAsXml(contentAsXml,"uft-8");
    }
    @Test
    public void contextLoads4() throws IOException {

        VfsFile vfs= VfsFactory.getVfs();
        try {
            vfs.startTransaction();
            VfsFile vfsFile = vfs.getVfsFile("/root/output.txt");
            OutputStream os = vfsFile.getOutPutStream();
            os.write("很舒服的sdsdsd".getBytes());
            os.close();
            vfs.commitTransaction();
        } catch (IOException e) {
            vfs.endTransaction();
        }

    }

}
