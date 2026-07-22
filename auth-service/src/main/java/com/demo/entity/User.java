package com.demo.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * Map trực tiếp vào bảng "users" sẵn có trong lms_db.
 *
 * Các cột role (user_role) và status (user_status) là PostgreSQL custom enum.
 * Dùng String trong Java để tránh type mismatch với Hibernate.
 * Validation thực hiện ở tầng service.
 */
@Entity
@Table(name = "users")
@Getter
@Setter
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String email;

    /**
     * Tên cột trong DB là password_hash (BCrypt hashed).
     * Không bao giờ lưu plaintext vào đây.
     */
    @Column(name = "password_hash", nullable = false)
    private String passwordHash;

    @Column(name = "full_name", nullable = false)
    private String fullName;

    @Column(name = "avatar_url")
    private String avatarUrl;

    private String phone;

    /**
     * PostgreSQL enum type: user_role
     * Giá trị: STUDENT | TEACHER | ADMIN
     * Map dưới dạng String, validate ở service layer.
     */
    @Column(name = "role", columnDefinition = "user_role")
    private String role;

    /**
     * PostgreSQL enum type: user_status
     * Giá trị: ACTIVE | INACTIVE | BANNED
     * Chỉ user ACTIVE mới được phép login.
     */
    @Column(name = "status", columnDefinition = "user_status")
    private String status;

    @Column(name = "reset_token")
    private String resetToken;

    @Column(name = "reset_token_expires_at")
    private LocalDateTime resetTokenExpiresAt;

    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
}
