package com.myflume.demo.handler;

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.servlet.http.HttpServletRequest;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * program: flume-study->MyHandler
 * description:自定义handler
 * 处理HTTP数据请求
 * author: gerry
 * created: 2020-04-18 12:48
 **/
public class HTTPSourceXMLHandler implements HTTPSourceHandler {
    /**
     * 假定xml请求格式，期望格式如下：
     *
     * <events>
     * <event>
     * <headers><header1>value1</header1></headers>
     * <body>test</body>
     * </event>
     * <event>
     * <headers><header1>value1</header1></headers>
     * <body>test2</body>
     * </event>
     * </events>
     */
    private final String ROOT = "events";
    private final String EVENT_TAG = "event";
    private final String HEADERS_TAG = "headers";
    private final String BODY_TAG = "body";

    private final String CONF_INSERT_TIMESTAMP = "insertTimestamp";
    private final String TIMESTAMP_HEADER = "timestamp";
    private final DocumentBuilderFactory documentBuilderFactory
            = DocumentBuilderFactory.newInstance();

    // Document builders are not thread-safe.
    // So make sure we have one for each thread.
    private final ThreadLocal<DocumentBuilder> docBuilder
            = new ThreadLocal<DocumentBuilder>();

    private boolean insertTimestamp;
    // 日志
    private static final Logger logger = LoggerFactory.getLogger(HTTPSourceXMLHandler.class);


    @Override
    public List<Event> getEvents(HttpServletRequest httpServletRequest) throws HTTPBadRequestException, Exception {
        if (docBuilder.get() == null) {
            docBuilder.set(documentBuilderFactory.newDocumentBuilder());
        }
        Document doc;
        final List<Event> events;
        try {

            // 获取输入流，解析xml文档格式
            doc = docBuilder.get().parse(httpServletRequest.getInputStream());
            // 获取根节点
            Element root = doc.getDocumentElement();

            root.normalize();
            // Verify that the root element is "events"
            // 校验标签
            Preconditions.checkState(
                    ROOT.equalsIgnoreCase(root.getTagName()));

            // 获取node节点
            NodeList nodes = root.getElementsByTagName(EVENT_TAG);
            logger.info("get nodes={}", nodes);

            // node节点个数
            int eventCount = nodes.getLength();
            // 创建node节点数组
            events = new ArrayList<Event>(eventCount);

            for (int i = 0; i < eventCount; i++) {
                // node节点元素
                Element event = (Element) nodes.item(i);
                // Get all headers. If there are multiple header sections,
                // combine them.
                NodeList headerNodes
                        = event.getElementsByTagName(HEADERS_TAG);
                Map<String, String> eventHeaders
                        = new HashMap<String, String>();
                for (int j = 0; j < headerNodes.getLength(); j++) {
                    Node headerNode = headerNodes.item(j);
                    NodeList headers = headerNode.getChildNodes();
                    for (int k = 0; k < headers.getLength(); k++) {
                        Node header = headers.item(k);
                        // Read only element nodes
                        if (header.getNodeType() != Node.ELEMENT_NODE) {
                            continue;
                        }
                        // Make sure a header is inserted only once,
                        // else the event is malformed
                        Preconditions.checkState(
                                !eventHeaders.containsKey(header.getNodeName()),
                                "Header expected only once " + header.getNodeName());
                        eventHeaders.put(
                                header.getNodeName(), header.getTextContent());
                    }
                }
                Node body = event.getElementsByTagName(BODY_TAG).item(0);
                if (insertTimestamp) {
                    eventHeaders.put(TIMESTAMP_HEADER, String.valueOf(System
                            .currentTimeMillis()));
                }
                events.add(EventBuilder.withBody(
                        body.getTextContent().getBytes(
                                httpServletRequest.getCharacterEncoding()),
                        eventHeaders));
            }


        } catch (SAXException ex) {
            throw new HTTPBadRequestException(
                    "Request could not be parsed into valid XML", ex);
        } catch (Exception ex) {
            throw new HTTPBadRequestException(
                    "Request is not in expected format. " +
                            "Please refer documentation for expected format.", ex);
        }
        return events;
    }

    @Override
    public void configure(Context context) {
        insertTimestamp = context.getBoolean(CONF_INSERT_TIMESTAMP, false);
    }
}
