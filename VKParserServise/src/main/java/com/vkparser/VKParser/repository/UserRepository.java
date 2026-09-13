package com.vkparser.VKParser.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.vkparser.VKParser.model.User;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {
    // Optional<User> findByVkUserId(Long vkUserId);
}