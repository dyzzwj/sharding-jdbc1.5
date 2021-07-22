package com.dyzwj.controller;


import com.dyzwj.entity.OrderEntity;
import com.dyzwj.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

    @Autowired
    private OrderMapper orderMapper;

    @GetMapping("/save")
    public String save(@RequestBody OrderEntity orderEntity) {
        orderMapper.save(orderEntity);
        return "ok";
    }

}
