package com.ksyun.dc.flume.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * program: flume-study->FileUtil
 * description: 文件工具类
 * author: gerry
 * created: 2020-04-21 15:21
 **/
public class FileUtils {
    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    public static String getFileByLine(String path) {
        String onLine = "";
        File file = new File(path);
        InputStream is = null;
        Reader reader = null;
        BufferedReader bufferedReader = null;
        try {
            is = new FileInputStream(file);
            reader = new InputStreamReader(is);
            bufferedReader = new BufferedReader(reader);
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                onLine = line;
                break;
            }
        } catch (FileNotFoundException e) {
            logger.info("sk文件不存在");
            logger.info(e.toString());
        } catch (IOException e) {
            logger.info("读取sk异常");
            logger.info(e.toString());
        } finally {
            try {
                if (null != bufferedReader) {
                    bufferedReader.close();
                }
                if (null != reader) {
                    reader.close();
                }
                if (null != is) {
                    is.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return onLine;
    }

    public static void main(String[] args) {
        String fileByLine = getFileByLine("/Users/gerry/Desktop/skyon/ccb/ak-sk.txt");
        System.out.println(fileByLine);
    }


}
