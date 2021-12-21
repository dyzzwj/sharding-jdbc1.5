package com.dyzwj.zwjsharding.entity;

import lombok.Data;


import java.util.Date;

@Data
public class User {

    private Long id;
    private String nickname;
    private String password;
    private Integer age;
    private Integer sex;
    private Date birthday;


}
