package com.dyzwj.zwjsharding.mapper;

import com.dyzwj.zwjsharding.entity.User;
import org.apache.ibatis.annotations.*;
import org.springframework.core.annotation.Order;

import java.util.List;

@Mapper
public interface UserMapper {


    @Insert("insert into t_user(nickname,password,age,sex,birthday) values(#{nickname},#{password},#{age},#{sex},#{birthday})")
    int insert(User user);

    @Delete("delete from t_user")
    int delete();


    @Select("select * from t_user")
    List<User> selectAll();


    List<User> selectIn(@Param("list") List<Integer> list);

    @Select("select * from t_user where age > #{age}")
    List<User> selectRange(Integer age );

    @Update("update t_user set nickname = #{nickname} where age = #{age}")
    int update(User user);




}
