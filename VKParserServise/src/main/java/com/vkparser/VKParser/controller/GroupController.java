package com.vkparser.VKParser.controller;

import com.vkparser.VKParser.model.Group;
import com.vkparser.VKParser.service.GroupService;

import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/groups")
public class GroupController {
    private final GroupService GroupService;
    
    public GroupController(GroupService GroupService) {
        this.GroupService = GroupService;
    }
}
