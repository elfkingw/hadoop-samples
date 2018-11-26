package com.newtouch.hdfs;

import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;


public class HdfsFileSystemServiceTest extends TestCase {

    private HdfsFileSystemService hdfsFileSystemService;
    
    private Logger logger = LoggerFactory.getLogger(HdfsFileSystemServiceTest.class);

    public void setUp() throws Exception {
        hdfsFileSystemService = new HdfsFileSystemService();
        super.setUp();
    }


    public void testMkdir() throws Exception {
        hdfsFileSystemService.mkdir("/home/test");
    }

    public void testListFile() throws Exception {
        String filePath = "/home";
        hdfsFileSystemService.listFile(filePath);
    }

    public void testDownload() throws Exception {
        String hdfsSrcFile = "/home/hadoop-hadoop-namenode-dev1.log";
        String dstFile = "/Users/elfkingw/desktop/hadoop-hadoop-namenode-dev1.log";
        File file = new File(dstFile);
        if (file.exists()) {
            file.delete();
        }
        hdfsFileSystemService.downloadFile(hdfsSrcFile, dstFile);
        if(file.exists()) {
            logger.info("从HDFS文件系统目录文件：{}下载到本地目录{}成功！", hdfsSrcFile, dstFile);
        }
        assertTrue(file.exists());
    }

        public void testUploadFile() throws Exception {
        URL url = hdfsFileSystemService.getClass().getClassLoader().getResource("helloworld.txt");
        if(url == null){
            throw new Exception("本地文件hellowrld.txt不存在！");
        }
        String srcFile = url.getPath();
        String dstFile = "/home/test/helloworld.txt";
        hdfsFileSystemService.uploadFile(srcFile, dstFile);
        FileSystem fileSystem = hdfsFileSystemService.getFileSystem();
        boolean isSuccess = fileSystem.exists(new Path(dstFile));
        if(isSuccess){
            logger.info("本地文件{}上传到HDFS文件系统{}成功",srcFile,dstFile);
        }
        assertTrue(fileSystem.exists(new Path(dstFile)));
    }

    public void testReadHdfsFile() {
        String filePath = "/home/test/helloworld.txt";
        String content = hdfsFileSystemService.readHdfsFile(filePath);
        logger.info("文件" + filePath + "读出的内容为\n" + content);
    }

    public void testDelete() throws Exception {
        String file = "/home/test";
        FileSystem fileSystem = hdfsFileSystemService.getFileSystem();
        if (!fileSystem.exists(new Path(file))) {
            return;
        }
        boolean isSuccess = hdfsFileSystemService.delete(file);
        assertTrue(isSuccess);
    }

}