package com.dyzwj.mapper;

import com.dyzwj.entity.OrderEntity;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface OrderMapper {


    @Insert("insert into t_order(order_id,user_id,statys) values(#{orderId},#{userId},#{status})")
    void save(OrderEntity entity);
}
