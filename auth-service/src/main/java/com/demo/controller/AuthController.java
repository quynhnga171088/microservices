package com.demo.controller;

import com.demo.dto.LoginRequest;
import com.demo.dto.LoginResponse;
import com.demo.service.AuthService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    /**
     * POST /auth/login
     *
     * Request body:
     * {
     *   "email": "user@example.com",
     *   "password": "plaintext-password"
     * }
     *
     * Response 200:
     * {
     *   "token": "eyJhbGciOiJIUzI1NiJ9...",
     *   "role": "STUDENT",
     *   "fullName": "Nguyen Van A",
     *   "email": "user@example.com"
     * }
     *
     * Response 401: sai credentials
     * Response 403: account bị khóa
     */
    @PostMapping("/login")
    public ResponseEntity<LoginResponse> login(@RequestBody LoginRequest request) {
        LoginResponse response = authService.login(request);
        return ResponseEntity.ok(response);
    }
}
