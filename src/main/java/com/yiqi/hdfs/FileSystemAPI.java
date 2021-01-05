package com.yiqi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;
import java.io.*;
import java.net.URI;

/**
 * @author Wanghy
 * @title: JavaAPI
 * @projectName hadoop
 * @description: FileSystem API学习
 * @date 2021/1/5
 */

public class FileSystemAPI {
    String url = "hdfs://namenode:9000/";
    String uri = "hdfs://namenode:9000/abd/anaconda-ks.cfg";
    String localSrc = "C:\\迅雷下载\\TakeColor.zip";
    String dst = "hdfs://namenode:9000/ccc/TakeColor.zip";
    String path = "hdfs://namenode:9000/ddd";
    /**
     * 读取数据
     * 使用seek()方法，将Hadoop文件系统中的一个文件在标准输出上显示两次。相对高开销，需要慎重使用。
     * @throws IOException
     */
    @Test
    public void readFile() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),configuration);
        FSDataInputStream in = null;
        try{
            /** FileSystem对象中的open()方法返回的是FSDataInputStream对象，而不是标准的java.io类对象。
             public class FSDataInputStream extends DataInputStream implements Seekable, PositionedReadable,
             ByteBufferReadable, HasFileDescriptor, CanSetDropBehind, CanSetReadahead, HasEnhancedByteBufferAccess,
             CanUnbuffer, StreamCapabilities{}
             public interface Seekable {
                 void seek(long var1) throws IOException;
                 long getPos() throws IOException;
                 @Private
                 boolean seekToNewSource(long var1) throws IOException;
             }
             Seekable接口支持在文件中找到指定文职，并提供一个查询当前唔知相对于文件起始位置偏移量(getPos())的查询方法。与java.io.InputStream的
             skip()不同，seek可以移到文件中任意一个绝对位置，skip()则只能相对于当前位置定位到另一个新位置。
             调用seek()来定位大于文件长度的位置会引发IOException异常。
             它是继承了java.io.DataInputStream的一个特殊类，并支持随机访问，由此可以从流的任意位置读取数据。
             public interface PositionedReadable {
                 int read(long position, byte[] buffer, int offset, int length) throws IOException;
                 void readFully(long var1, byte[] var3, int var4, int var5) throws IOException;
                 void readFully(long var1, byte[] var3) throws IOException;
             }
             PositionedReadable支持从一个指定偏移量处读取文件的一部分。
             read()从文件指定的position处读取至多为length字节的数据并存入缓冲区buffer的制定偏移量offset处。
             */
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in,System.out,4096,false);
            in.seek(0); // 回到文件开始的地方
            IOUtils.copyBytes(in,System.out,4096,false);
        }finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 写入数据
     * 给准备建的文件指定一个Path对象，然后返回一个用于写入数据的输出流。
     * create()能够为需要写入且当前不存在的文件创建父目录。如果希望父目录不存在就导致文件写入失败，则应该先调用exists()方法
     * 缉拿查父目录是否存在。另一种方案是使用FileContext
     * Progressable()用于传递回调接口，可以把数据写入datanode的进度通知给应用
     */
    @Test
    public void fileCopyWithProgress() throws IOException{
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(dst),configuration);
        OutputStream out = fileSystem.create(new Path(dst), new Progressable() {
            public void progress(){
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in,out,4096,true);
    }

    /**
     * 目录创建
     * @throws IOException
     */
    @Test
    public void mkdirs() throws IOException{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(url),configuration);
        fileSystem.mkdirs(new Path(path));
    }

    /**
     * 删除数据
     * @throws IOException
     */
    @Test
    public void delete() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(url),configuration);
        fileSystem.deleteOnExit(new Path(dst));
    }

    /**
     * 显示Hadoop文件系统中一组路径的文件信息
     * 使用通配符以及PathFilter可以对文件进行过滤
     */
    @Test
    public void listStatus() throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(url),new Configuration());
        Path[] paths = {new Path(url)};
        FileStatus[] fileStatus = fileSystem.listStatus(paths);
        Path[] listedPaths = FileUtil.stat2Paths(fileStatus);
        for (Path p :listedPaths){
            System.out.println(p);
        }
        System.out.println("==========================");
        fileStatus = fileSystem.globStatus(new Path("/2020/*/*"),new RegexExcludePathFilter("^.*/2020/12/31$"));
        listedPaths = FileUtil.stat2Paths(fileStatus);
        for (Path p :listedPaths){
            System.out.println(p);
        }
    }


}
