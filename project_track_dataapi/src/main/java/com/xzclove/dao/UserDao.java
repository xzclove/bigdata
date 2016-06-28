package com.xzclove.dao;

import java.util.List;

import com.xzclove.model.User;

/**
 * 操作user的dao接口
 * 
 * @author gerry
 *
 */
public interface UserDao {
	User getOneUser();

	List<User> getAllUsers();
}
