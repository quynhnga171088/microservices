package com.demo.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LoginResponse {

    /** JWT token — client lưu lại và gửi trong header Authorization: Bearer <token> */
    private String token;

    /** Vai trò user: STUDENT | TEACHER | ADMIN */
    private String role;

    /** Tên hiển thị */
    private String fullName;

    /** Email (cũng là subject trong JWT) */
    private String email;
}
