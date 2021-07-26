package com.dyzwj.service;

import com.dyzwj.entity.User;

import java.util.List;

public interface UserService {

    /**
     * add a new user
     * @param user
     * @return
     */
    Integer add(User user);
    /**
     * select all users
     * @return
     */
    List<User> selectAll();
}
