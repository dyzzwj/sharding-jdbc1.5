package com.dyzwj.mapper;

import com.dyzwj.entity.User;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface UserMapper {
    /**
     * add a new user
     * @param user
     * @return
     */
    @Insert("insert into t_user(username,password) values(#{username},#{password})")
    Integer add(User user);
    /**
     * select all users
     * @return
     */
    @Select("select * from t_user")
    List<User> selectAll();
}
