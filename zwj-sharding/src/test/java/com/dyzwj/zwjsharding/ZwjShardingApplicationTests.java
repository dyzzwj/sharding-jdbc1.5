package com.dyzwj.zwjsharding;

import com.dyzwj.zwjsharding.entity.User;
import com.dyzwj.zwjsharding.mapper.UserMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

@SpringBootTest
class ZwjShardingApplicationTests {

    @Test
    void contextLoads() {
    }

    @Autowired
    UserMapper userMapper;

    @Test
    public void testInsert(){
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {

            int ageInt = random.nextInt(1000);
            int sexInt = random.nextInt(100);
            System.out.println(ageInt);
            System.out.println(sexInt);
            System.out.println();
            User user = new User();
            user.setAge(ageInt % 100);
            user.setSex(sexInt % 2);
            user.setNickname("aa" + i);
            user.setPassword("dyzwj");
//            user.setBirthday(LocalDateTime.now());
            userMapper.insert(user);
        }
    }

    @Test
    public void testInsert1(){
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {

            System.out.println();
            System.out.println();
            User user = new User();
            user.setAge(random.nextInt() % 100);
            user.setSex(random.nextInt() % 2);
            user.setNickname("aa" + i);
            user.setPassword("dyzwj");
//            user.setBirthday(LocalDateTime.now());
            userMapper.insert(user);
        }
    }

    @Test
    public void testDelete(){
        userMapper.delete();
    }

    @Test
    public void testUpdate(){
        User user =  new User();
        user.setNickname("郑文杰");
        user.setAge(100);
        userMapper.update(user);
    }

    @Test
    public void testSelectAll(){
        List<User> users = userMapper.selectAll();
        users.forEach(System.out::println);
    }

    @Test
    public void testSelectIn(){
        List<User> result = userMapper.selectIn(Arrays.asList(10, 20));
        result.forEach(System.out::println);

    }



    @Test
    public void testRange(){
        List<User> result = userMapper.selectRange(50);
        result.forEach(System.out::println);

    }



}
