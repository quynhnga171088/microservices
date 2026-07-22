package com.demo.dto;

import lombok.Data;

@Data
public class LoginRequest {
    private String email;
    private String password;  // plaintext — sẽ được BCrypt.matches() kiểm tra
}
