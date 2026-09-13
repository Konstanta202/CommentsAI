package com.vkparser.VKParser.controller;

import com.vkparser.VKParser.model.Psot;
import com.vkparser.VKParser.service.PostService;

public class PostController {
        private final PostService postService;

    public PostController(PostService postService) {
        this.postService = postService;
    }
}
