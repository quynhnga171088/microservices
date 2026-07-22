package com.demo.repository;

import com.demo.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {

    /**
     * Tìm user theo email (dùng cho login).
     * Spring Data JPA tự generate query: SELECT * FROM users WHERE email = ?
     */
    Optional<User> findByEmail(String email);
}
