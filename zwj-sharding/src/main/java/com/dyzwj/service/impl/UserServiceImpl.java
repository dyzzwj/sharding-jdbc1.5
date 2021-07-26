package com.dyzwj.service.impl;

import com.dyzwj.entity.User;
import com.dyzwj.mapper.UserMapper;
import com.dyzwj.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserServiceImpl implements UserService {

    @Autowired
    private UserMapper userMapper;

    @Override
    public Integer add(User user) {

        return userMapper.add(user);
    }

    @Override
    public List<User> selectAll() {
        return userMapper.selectAll();
    }
}
