package com.dyzwj;

import com.dyzwj.entity.User;
import com.dyzwj.service.UserService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.UUID;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@SpringBootTest(classes = UserApplication.class)
public class UserTest {

    private static Logger logger = LoggerFactory.getLogger(UserTest.class);


    @Autowired
    private UserService userService;

    @Test
    public void testAdd(){

        String username = UUID.randomUUID().toString();
        String password = UUID.randomUUID().toString();
        User user = new User(username, password);
        userService.add(user);
        logger.info("userId: {}", user.getUserId());

    }

    @Test
    public void testBatchAdd(){

        for (int i = 0; i < 8; i++) {
            String username = UUID.randomUUID().toString();
            String password = UUID.randomUUID().toString();
            User user = new User(username, password);
            userService.add(user);
            logger.info("userId: {}", user.getUserId());

        }

    }

    /* 测试查询 */
    @Test
    public void testSelect()
    {
        List<User> users = userService.selectAll();
        logger.info("Total records: {}", users.size());
        for (User user : users) {
            logger.info("{}", user);
        }
    }



}
