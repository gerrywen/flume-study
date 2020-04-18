package com.myflume.springboot.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * description:
 *
 * @author wenguoli
 * @date 2020/3/3 14:40
 */
@RestController
@RequestMapping("/")
public class IndexController {
    @GetMapping("")
    public String test() {
        return "hello ，springboot";
    }
}
