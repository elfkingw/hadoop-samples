package com.newtouch.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
    public boolean mkdir(String filepath) {
        boolean isSuccess  = false;
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            if (fileSystem.exists(new Path(genHdfsPath(filepath)))) {
                logger.error("{}文件夹已存在！", filepath);
                return false;
            } else {
                logger.info("{}文件夹不存在！", filepath);
            }
//            FsPermission filePermission = null;
//            filePermission = new FsPermission(
//                    FsAction.ALL, //user action
//                    FsAction.ALL, //group action
//                    FsAction.READ);//other action
            //创建目录 不设置权限,默认为当前hdfs服务器启动用户
            isSuccess = fileSystem.mkdirs(new Path(genHdfsPath(filepath)));
            if (isSuccess) {
                logger.info("文件夹{}创建成功！", filepath);
            } else {
                logger.error("文件夹{}创建失败！", filepath);
            }
        } catch (IOException e) {
             logger.error("HDFS文件操作失败", e);
        } finally {
            closeFileSystem(fileSystem);
        }
        return isSuccess;
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
                logger.error("文件：{}删除失败！", file);
            }
        } catch (IOException e) {
             logger.error("HDFS文件操作失败", e);
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
             logger.error("HDFS文件操作失败", e);
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
             logger.error("HDFS文件操作失败", e);
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
             logger.error("HDFS文件操作失败", e);
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
             logger.error("HDFS文件操作失败", e);
        } finally {
            try {
                if (inputSteam != null) {
                    inputSteam.close();
                }
            } catch (IOException e) {
                 logger.error("HDFS文件操作失败", e);
            }
            closeFileSystem(fileSystem);
        }
        return content;
    }

    /**
     * 获取文件块位置
     *
     * @param file  文件
     * @param start 开始位置
     * @param len   长度
     * @return
     */
    public BlockLocation[] getFileBlockLocation(String file, long start, long len) {
        BlockLocation[] fileBlockLocations = null;
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            fileBlockLocations = fileSystem.getFileBlockLocations(new Path(file), start, len);
        } catch (IOException e) {
            logger.error("HDFS文件操作失败", e);
        } finally {
            closeFileSystem(fileSystem);
        }
        return fileBlockLocations;
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
            logger.error("HDFS文件操作失败", e);
        }
    }
}
