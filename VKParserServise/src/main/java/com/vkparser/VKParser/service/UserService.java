package com.vkparser.VKParser.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.vkparser.VKParser.model.User;
import com.vkparser.VKParser.repository.UserRepository;

import jakarta.transaction.Transactional;


@Service
@Transactional
public class UserService {

    private final UserRepository userRepository;


    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }
    
    public List<User> getUsers() {
        return userRepository.findAll();
    }


    // public User createUser(User user) {
    //     Optional<User> optionalUser =  userRepository.findByEmail(user.getEmail());
    //     if (optionalUser.isPresent()) {
    //         throw new IllegalStateException("user with this email already exists");
    //     }
    //     return userRepository.save(user);
    // }


    // public void deleteUser(Long id) {
    //     Optional<User> optionalUser = userRepository.findById(id);
    //     if (optionalUser.isEmpty()) {
    //         throw new IllegalStateException("user not found");
    //     } 
    //     userRepository.deleteById(id);
    // }

    // public void updateUser(Long id, String name, String email, LocalDate birth) {
    //     Optional<User> optionalUser = userRepository.findById(id);
    //     if (optionalUser.isEmpty()) {
    //         throw new IllegalStateException("user not found");
    //     }
    //     User user = optionalUser.get();
    //     if (email != null && !email.equals(user.getEmail())) {
    //         Optional<User> optionalEmail =  userRepository.findByEmail(email);
    //         if (optionalEmail.isPresent()) {
    //             throw new IllegalStateException("user with this email already exists");
    //         }
    //         user.setEmail(email);
    //     }
    //     if (name != null && !name.equals(user.getName())) {
    //         user.setName(name);
    //     }

    //     if (birth != null && !birth.equals(user.getBirth())) {
    //         user.setBirth(birth);
    //     }
    //     userRepository.save(user);
    // }

}
