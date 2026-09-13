package com.vkparser.VKParser.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.vkparser.VKParser.model.Comment;

@Repository
public interface CommentRepository extends JpaRepository<Comment, Long> {
    
    List<Comment> findByUserId(Long userId);
    
    boolean existsByUserId(Long userId);
    
    long countByUserId(Long userId);
    
    List<Comment> findByPostIdAndUserId(Long postId, Long userId);
    
    List<Comment> findByPostId(Long postId);
}