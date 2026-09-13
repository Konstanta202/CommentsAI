package com.vkparser.VKParser.controller;

import java.time.LocalDate;
import java.util.List;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.vkparser.VKParser.model.User;
import com.vkparser.VKParser.service.UserService;

@RestController
@RequestMapping(path = "api/users")
public class UserController {

    private final UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    @GetMapping("/")
    public List<User> getUsers() {
        return userService.getUsers();
    }

    // @PostMapping("/create")
    // public User createUser(@RequestBody User user) {
    //     return userService.createUser(user);
    // }

    // @DeleteMapping(path = "delete/{id}")
    // public void deleteUser(@PathVariable Long id) {
    //     userService.deleteUser(id);
    // }

    // @PutMapping(path = "update/{id}")
    // public void updateUser(
    //         @PathVariable Long id,
    //         @RequestParam(required =  false) String name,
    //         @RequestParam(required =  false) String email,
    //         @RequestParam(required =  false) LocalDate birth) {
    //     userService.updateUser(id, name, email, birth);
    // }


}
