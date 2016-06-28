package com.xzclove.dao.impl;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.xzclove.dao.UserDao;
import com.xzclove.dao.mybatis.MyBatisDao;
import com.xzclove.model.User;

@Repository
public class UserDaoImpl extends MyBatisDao implements UserDao {

	public UserDaoImpl() {
		super(User.class.getName());
		// 此时命名空间为: com.beifeng.model.User
	}

	public User getOneUser() {
		int id = 1;
		return super.get("getOneUser", id);
	}

	public List<User> getAllUsers() {
		return super.getList("getAllUsers", null);
	}

}
