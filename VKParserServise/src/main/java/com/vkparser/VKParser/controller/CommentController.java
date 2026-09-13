package com.vkparser.VKParser.controller;

import com.vkparser.VKParser.model.Comment;
import com.vkparser.VKParser.service.CommentService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/comments")
public class CommentController {
    
    private final CommentService commentService;
    
    public CommentController(CommentService commentService) {
        this.commentService = commentService;
    }
    
    @GetMapping("/user/{userId}")
    public List<Comment> getCommentsByUser(@PathVariable Long userId) {
        return commentService.getCommentsByUserId(userId);
    }
    
    @GetMapping("/user/{userId}/count")
    public long countUserComments(@PathVariable Long userId) {
        return commentService.countUserComments(userId);
    }
}