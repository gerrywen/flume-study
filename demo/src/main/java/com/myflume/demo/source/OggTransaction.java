package com.myflume.demo.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * @program: flume-study->OggTransaction
 * @description:
 * @author: gerry
 * @created: 2020-09-03 16:01
 **/
public class OggTransaction {
    /**
     * 事务ID
     */
    private Long transactionId = 0L;

    /**
     * 顺序值
     */
    private Long orderId = 0L;

    /**
     * 标记开始位置
     */
    private int bCount = 0;
    /**
     * 标记结束位置
     */
    private int cCount = 0;

    public Long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(Long transactionId) {
        this.transactionId = transactionId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public int getbCount() {
        return bCount;
    }

    public void setbCount(int bCount) {
        this.bCount = bCount;
    }

    public int getcCount() {
        return cCount;
    }

    public void setcCount(int cCount) {
        this.cCount = cCount;
    }


    public String getFileContent(String fileName) {
        StringBuffer content = new StringBuffer();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(new File(fileName)));
            String line = null;
            while ((line = reader.readLine()) != null) {
                content.append(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return content.toString();
    }

    @Override
    public String toString() {
        return "OggTransaction{" +
                "transactionId=" + transactionId +
                ", orderId=" + orderId +
                ", bCount=" + bCount +
                ", cCount=" + cCount +
                '}';
    }
}
