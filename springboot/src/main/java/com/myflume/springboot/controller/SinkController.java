package com.myflume.springboot.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

/**
 * program: flume-study->SinkController
 * description: sink请求控制
 * author: gerry
 * created: 2020-04-18 23:54
 **/
@Controller
@RequestMapping("/sink")
public class SinkController {

    @PostMapping("")
    @ResponseBody
    public void sinkInfo(HttpServletRequest request, @RequestBody String jsonObject) {
        //获取表单中所有文本域的name
        System.out.println("=========================FLUME==================");
        System.out.println(jsonObject);
        System.out.println("=========================END FLUME===============");
    }
}
