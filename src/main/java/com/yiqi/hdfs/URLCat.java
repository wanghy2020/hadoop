package com.yiqi.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @author Wanghy
 * @title: URLCat
 * @projectName hadoop
 * @description: 通过URLStreamHandler实例以标准输出方式显示Hadoop文件系统的文件
 * @date 2021/1/5
 */
public class URLCat {
    /**
     Hadoop权威指南~让Java程序能够识别Hadoop的hdfs URL方案还需要一些额外的工作。这里采用的方法是通过FsUrlStreamHandlerFactory实例
     调用java.net.URL对象的setURLStreamHandlerFactory()方法。每个java虚拟机只能调用一次这个方法，因此通常在静态方法中调用。
     */
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) {
        InputStream in = null;
        try {
            in = new URL("hdfs://namenode:9000/abd/anaconda-ks.cfg").openStream();
            IOUtils.copyBytes(in,System.out,4096,false);
        } catch (MalformedURLException malformedURLException) {
            malformedURLException.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
