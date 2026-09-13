package com.vkparser.VKParser.service;

import com.vkparser.VKParser.model.Comment;
import com.vkparser.VKParser.repository.CommentRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CommentService {
    
    private final CommentRepository commentRepository;
    
    public CommentService(CommentRepository commentRepository) {
        this.commentRepository = commentRepository;
    }
    
    public List<Comment> getCommentsByUserId(Long userId) {
        return commentRepository.findByUserId(userId);
    }
    

    public boolean userHasComments(Long userId) {
        return commentRepository.existsByUserId(userId);
    }
    
    public long countUserComments(Long userId) {
        return commentRepository.countByUserId(userId);
    }
    public List<Comment> getUserCommentsForPost(Long postId, Long userId) {
        return commentRepository.findByPostIdAndUserId(postId, userId);
    }
}