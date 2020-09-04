/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.myflume.demo.source;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.BYTE_OFFSET_HEADER_KEY;

/**
 * 读取文件位置标记
 */
public class OggTailFile {
    private static final Logger logger = LoggerFactory.getLogger(OggTailFile.class);

    private static final byte BYTE_NL = (byte) 10;
    private static final byte BYTE_CR = (byte) 13;

    private static final int BUFFER_SIZE = 8192;
    private static final int NEED_READING = -1;

    private RandomAccessFile raf;
    private final String path;
    private final long inode;
    private long pos;
    private long lastUpdated;
    private boolean needTail;
    private final Map<String, String> headers;
    private byte[] buffer;
    private byte[] oldBuffer;
    private int bufferPos;
    private long lineReadPos;

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

    /**
     * 文件标记路径
     */
    private String positionFilePath;

    /**
     * 事务路径
     */
    private String transactionFilePath;

    public OggTailFile(File file, Map<String, String> headers, long inode, long pos, String positionFilePath)
            throws IOException {
        this.raf = new RandomAccessFile(file, "r");
        if (pos > 0) {
            raf.seek(pos);
            lineReadPos = pos;
        }
        this.path = file.getAbsolutePath();
        this.inode = inode;
        this.pos = pos;
        this.lastUpdated = 0L;
        this.needTail = true;
        this.headers = headers;
        this.oldBuffer = new byte[0];
        this.bufferPos = NEED_READING;
        this.positionFilePath = positionFilePath;

        // 加载本地文件数据
        loadPositionFile(this.positionFilePath);
        writeTransaction();

        logger.info("new OggTailFile info: " + file + ", inode: " + inode + ", pos: " + pos
                + ", transactionId: " + transactionId + ", orderId: " + orderId
                + ", bCount: " + bCount + ", cCount: " + cCount + ", positionFilePath: " + positionFilePath);
    }

    public RandomAccessFile getRaf() {
        return raf;
    }

    public String getPath() {
        return path;
    }

    public long getInode() {
        return inode;
    }

    public long getPos() {
        return pos;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public boolean needTail() {
        return needTail;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public long getLineReadPos() {
        return lineReadPos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public void setNeedTail(boolean needTail) {
        this.needTail = needTail;
    }

    public void setLineReadPos(long lineReadPos) {
        this.lineReadPos = lineReadPos;
    }

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

    public String getPositionFilePath() {
        return positionFilePath;
    }

    public void setPositionFilePath(String positionFilePath) {
        this.positionFilePath = positionFilePath;
    }

    public String getTransactionFilePath() {
        return transactionFilePath;
    }

    public void setTransactionFilePath(String transactionFilePath) {
        this.transactionFilePath = transactionFilePath;
    }

    public boolean updatePos(String path, long inode, long pos) throws IOException {
        if (this.inode == inode && this.path.equals(path)) {
            setPos(pos);
            updateFilePos(pos);
            logger.info("Updated position, file: " + path + ", inode: " + inode + ", pos: " + pos
                    + ", transactionId: " + transactionId + ", orderId: " + orderId
                    + ", bCount: " + bCount + ", cCount: " + cCount);
            return true;
        }
        return false;
    }

    public void updateFilePos(long pos) throws IOException {
        raf.seek(pos);
        lineReadPos = pos;
        bufferPos = NEED_READING;
        oldBuffer = new byte[0];
    }


    public List<Event> readEvents(int numEvents, boolean backoffWithoutNL,
                                  boolean addByteOffset) throws IOException {
        List<Event> events = Lists.newLinkedList();
        logger.info("readEvents numEvents >>>>>>>>> " + numEvents);
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent(backoffWithoutNL, addByteOffset);

            if (event == null) {
                break;
            }

            String tempStr = new String(event.getBody());
            logger.info("批量读取readEvents >>>>>>" + tempStr);
            if (tempStr.startsWith("B")) {
                break;
            } else if (tempStr.startsWith("C")) {
                // 获取列表最后一条数据
                Event lastEvent = events.remove(events.size() - 1);
                if (lastEvent != null) {
                    String lastBody = new String(lastEvent.getBody());

                    lastBody = lastBody.substring(5, lastBody.length());
                    tempStr = "true" + lastBody;

                    // json转map
//                    Gson gson = new Gson();
//                    Map<String, Object> map = new HashMap<String, Object>();
//                    map = gson.fromJson(lastBody, map.getClass());
//                    map.put("end_transaction", "true");
//
//                    // 处理完的数据重新赋值body
//                    tempStr = map.toString();
                    event.setBody(tempStr.getBytes());
                    // 重新赋值发送数据
                    logger.info("重新赋值数据 >>>>>>" + tempStr);
                    events.add(event);
                }
            } else {
//                Map<String, Object> tmpMap = new HashMap<>();
//                // 数据处理
//                Map<String, String> dataMap = new HashMap<>();
//                // 分割字符
//                String[] splitOgg = tempStr.split("\\|");
//                String[] keys = {"end_transaction", "transaction_sort", "data_sort", "type", "op_type", "table", "op_ts"};
//                for (int j = 0; j < splitOgg.length; j = j + 1) {
//                    if (j < keys.length) {
//                        tmpMap.put(keys[j], splitOgg[j]);
//                    }
//                }
//                // 处理剩余的键值对
//                for (int j = 7; j < splitOgg.length; j = j + 2) {
//                    String key = splitOgg[j];
//                    String value = "";
//                    if (j + 1 < splitOgg.length) {
//                        value = splitOgg[j + 1];
//                    }
//                    dataMap.put(key, value);
//                }
//                tmpMap.put("data", dataMap);
//                // 处理完的数据重新赋值body
//                Gson gson = new Gson();
//                tempStr = gson.toJson(tmpMap);

                event.setBody(tempStr.getBytes());
                events.add(event);
            }
        }
        // todo:更新数据写入文件
        writeTransaction();
        return events;
    }

    private Event readEvent(boolean backoffWithoutNL, boolean addByteOffset) throws IOException {
        logger.info("OggTailFile readEvent path: " + path);

        Long posTmp = getLineReadPos();
        LineResult line = readLine();
        if (line == null) {
            return null;
        }
        if (backoffWithoutNL && !line.lineSepInclude) {
            logger.info("Backing off in file without newline: "
                    + path + ", inode: " + inode + ", pos: " + raf.getFilePointer());
            updateFilePos(posTmp);
            return null;
        }

        Event event = EventBuilder.withBody(line.line);
        String tempStr = new String(event.getBody());
        // 处理每个事物里面的数据
        long updateOrderId;
        if (tempStr.startsWith("B")) {
            bCount += 1;
            setbCount(bCount);
            updateOrderId = 0L;
            // 记录事物位置值
            this.setOrderId(updateOrderId);

        } else if (tempStr.startsWith("C")) {
            cCount += 1;
            setcCount(cCount);
            updateOrderId = -1L;
            // 记录事物位置值
            this.setOrderId(updateOrderId);
        } else {
            Long dataOrder = this.getOrderId();
            dataOrder += 1L;
            updateOrderId = dataOrder;
            // 记录事物位置值
            this.setOrderId(updateOrderId);
            tempStr = "false|" + this.getTransactionId() + "|" + this.getOrderId() + "|" + tempStr;
        }

        logger.info("OggTailFile get event singleBody data >>>>>>>>>>>>>> : " + tempStr);
        // 结束一个事物
        if (bCount == 1 && cCount == 1) {
            // 完整事物结束，清空，重新计算
            bCount = cCount = 0;
            setbCount(bCount);
            setcCount(cCount);
            Long transactionOrder = this.getTransactionId();
            transactionOrder += 1L;
            long transactionId = transactionOrder;
            this.setTransactionId(transactionId);
        }

        // 重新赋值发送数据
        event.setBody(tempStr.getBytes());
        if (addByteOffset == true) {
            event.getHeaders().put(BYTE_OFFSET_HEADER_KEY, posTmp.toString());
        }
        return event;
    }

    private void readFile() throws IOException {
        if ((raf.length() - raf.getFilePointer()) < BUFFER_SIZE) {
            buffer = new byte[(int) (raf.length() - raf.getFilePointer())];
        } else {
            buffer = new byte[BUFFER_SIZE];
        }
        raf.read(buffer, 0, buffer.length);
        bufferPos = 0;
    }

    private byte[] concatByteArrays(byte[] a, int startIdxA, int lenA,
                                    byte[] b, int startIdxB, int lenB) {
        byte[] c = new byte[lenA + lenB];
        System.arraycopy(a, startIdxA, c, 0, lenA);
        System.arraycopy(b, startIdxB, c, lenA, lenB);
        return c;
    }

    public LineResult readLine() throws IOException {
        LineResult lineResult = null;
        while (true) {
            if (bufferPos == NEED_READING) {
                if (raf.getFilePointer() < raf.length()) {
                    readFile();
                } else {
                    if (oldBuffer.length > 0) {
                        lineResult = new LineResult(false, oldBuffer);
                        oldBuffer = new byte[0];
                        setLineReadPos(lineReadPos + lineResult.line.length);
                    }
                    break;
                }
            }
            for (int i = bufferPos; i < buffer.length; i++) {
                if (buffer[i] == BYTE_NL) {
                    int oldLen = oldBuffer.length;
                    // Don't copy last byte(NEW_LINE)
                    int lineLen = i - bufferPos;
                    // For windows, check for CR
                    if (i > 0 && buffer[i - 1] == BYTE_CR) {
                        lineLen -= 1;
                    } else if (oldBuffer.length > 0 && oldBuffer[oldBuffer.length - 1] == BYTE_CR) {
                        oldLen -= 1;
                    }
                    lineResult = new LineResult(true,
                            concatByteArrays(oldBuffer, 0, oldLen, buffer, bufferPos, lineLen));
                    setLineReadPos(lineReadPos + (oldBuffer.length + (i - bufferPos + 1)));
                    oldBuffer = new byte[0];
                    if (i + 1 < buffer.length) {
                        bufferPos = i + 1;
                    } else {
                        bufferPos = NEED_READING;
                    }
                    break;
                }
            }
            if (lineResult != null) {
                break;
            }
            // NEW_LINE not showed up at the end of the buffer
            oldBuffer = concatByteArrays(oldBuffer, 0, oldBuffer.length,
                    buffer, bufferPos, buffer.length - bufferPos);
            bufferPos = NEED_READING;
        }
        return lineResult;
    }

    public void close() {
        try {
            raf.close();
            raf = null;
            long now = System.currentTimeMillis();
            setLastUpdated(now);
        } catch (IOException e) {
            logger.error("Failed closing file: " + path + ", inode: " + inode, e);
        }
    }

    private class LineResult {
        final boolean lineSepInclude;
        final byte[] line;

        public LineResult(boolean lineSepInclude, byte[] line) {
            super();
            this.lineSepInclude = lineSepInclude;
            this.line = line;
        }
    }

    public void loadPositionFile(String filePath) {
        File file1 = new File(filePath);
        String transactionFile = file1.getParent().replace('\\', '/');
        this.transactionFilePath = transactionFile + "/taildir_transaction.json";
        File file = new File(this.transactionFilePath);
        if (file.exists()) {
            OggTransaction oggTransaction = new OggTransaction();
            String content = oggTransaction.getFileContent(this.transactionFilePath);
            Gson gson = new Gson();
            OggTransaction fromJson = gson.fromJson(content, OggTransaction.class);
            logger.info("fromJson >>>>>> :" + fromJson);
            setTransactionId(fromJson.getTransactionId());
            setOrderId(fromJson.getOrderId());
            setbCount(fromJson.getbCount());
            setcCount(fromJson.getcCount());
        }
    }

    private void writeTransaction() {
        File file1 = new File(positionFilePath);
        String transactionFile = file1.getParent().replace('\\', '/');
        this.transactionFilePath = transactionFile + "/taildir_transaction.json";
        setTransactionFilePath(this.transactionFilePath);
        File file = new File(this.transactionFilePath);
        FileWriter writer = null;
        try {
            writer = new FileWriter(file);
            String json = toTransactionJson();
            writer.write(json);
        } catch (Throwable t) {
            logger.error("Failed writing positionFile", t);
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                logger.error("Error: " + e.getMessage(), e);
            }
        }
    }


    private String toTransactionJson() {
//        @SuppressWarnings("rawtypes")
//        List<Map> posInfos = Lists.newArrayList();
        Map<String, Object> map = new HashMap<>();
        map.put("transactionId", this.getTransactionId());
        map.put("orderId", this.getOrderId());
        map.put("bCount", this.getbCount());
        map.put("cCount", this.getcCount());
        logger.info("toTransactionJson map >>>>> :" + map.toString());
//        posInfos.add(ImmutableMap.of("transactionId", this.getTransactionId(),
//                "orderId", this.getOrderId(), "bCount", this.getbCount(),
//                "cCount", this.getcCount()));
        return new Gson().toJson(map);
    }
}
