package com.newtouch.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HDFS文件系统java操作
 */
public class HdfsFileSystemService {

    private Logger logger = LoggerFactory.getLogger(HdfsFileSystemService.class);
    /**
     *
     */
    private final static String HDFS_ROOT_URL = "hdfs://dev1:9000";

    /**
     * 获取HDFS文件系统
     *
     * @return HDFS文件系统
     * @throws IOException 抛出异常
     */
    public FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_ROOT_URL);
        return FileSystem.get(conf);
    }

    /**
     * 创建目录
     *
     * @param filepath 要创建的文件目录
     */
    public void mkdir(String filepath) {
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            if (fileSystem.exists(new Path(genHdfsPath(filepath)))) {
                logger.info("{}文件夹已存在！", filepath);
                return;
            } else {
                logger.info("{}文件夹不存在！", filepath);
            }
//            FsPermission filePermission = null;
//            filePermission = new FsPermission(
//                    FsAction.ALL, //user action
//                    FsAction.ALL, //group action
//                    FsAction.READ);//other action
            //创建目录 不设置权限,默认为当前hdfs服务器启动用户
            boolean isSuccess = fileSystem.mkdirs(new Path(genHdfsPath(filepath)));
            if (isSuccess) {
                logger.info("文件夹{}创建成功！", filepath);
            } else {
                logger.info("文件夹{}创建失败！", filepath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeFileSystem(fileSystem);
        }
    }

    /**
     * 删除文件
     *
     * @param file HDFS文件系统中文件
     * @return 是否删除成功
     */
    public boolean delete(String file) {
        boolean isSuccess = false;
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            //delete第二参数为是否遍历删除
            isSuccess = fileSystem.delete(new Path(file), true);
            if (isSuccess) {
                logger.info("文件:{}删除成功！", file);
            } else {
                logger.info("文件：{}删除失败！", file);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeFileSystem(fileSystem);
        }
        return isSuccess;
    }

    /**
     * 遍历文件夹下文件
     *
     * @param filePath HDFS文件系统中文件路径
     */
    public void listFile(String filePath) {
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(filePath));
            for (FileStatus filestatus : fileStatuses) {
                logger.info(filestatus.getPath().getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeFileSystem(fileSystem);
        }
    }

    /**
     * hdfs文件系统中的文件拷贝到本地
     *
     * @param srcPath HDFS文件系统中路劲
     * @param dstPath 本地目标路径
     */
    public void downloadFile(String srcPath, String dstPath) {
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            fileSystem.copyToLocalFile(new Path(srcPath), new Path(dstPath));

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeFileSystem(fileSystem);
        }
    }

    /**
     * 拷贝本地服务器文件到hdfs文件系统
     *
     * @param srcFile 本地文件路径
     * @param dstFile hdfs文件系统中目标文件路径
     */
    public void uploadFile(String srcFile, String dstFile) {
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            fileSystem.copyFromLocalFile(new Path(srcFile), new Path(genHdfsPath(dstFile)));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeFileSystem(fileSystem);
        }
    }

    /**
     * 将入路劲加上hdfs前缀
     *
     * @param path HDFS文件系统中路径
     * @return 返回增加HDFS前缀路径
     */
    public String genHdfsPath(String path) {
        String hdfsPath;
        if (path.startsWith("/")) {
            hdfsPath = HDFS_ROOT_URL + path;
        } else {
            hdfsPath = HDFS_ROOT_URL + "/" + path;
        }
        return hdfsPath;
    }

    /**
     * 读取HDFS文件系统的文件返回内容字符串
     *
     * @param file HDFS中文件
     * @return 文件内容信息
     */
    public String readHdfsFile(String file) {
        FSDataInputStream inputSteam = null;
        String content = null;
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            inputSteam = fileSystem.open(new Path(genHdfsPath(file)));
            content = IOUtils.toString(inputSteam, "utf-8");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (inputSteam != null) {
                    inputSteam.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            closeFileSystem(fileSystem);
        }
        return content;
    }

    /**
     * 关闭fileSystem
     *
     * @param fileSystem HDFS文件系统
     */
    private void closeFileSystem(FileSystem fileSystem) {
        try {
            if (fileSystem != null) {
                fileSystem.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
