-- Test SQL file for pgrpc query generation feature

-- name: GetUserById :one
-- Fetch a single user by ID using named parameter
SELECT id, username, email, created_at
FROM users
WHERE id = @user_id
LIMIT 1;

-- name: GetUserByEmail :one
-- Fetch user by email
SELECT id, username, email
FROM users
WHERE email = @email;

-- name: ListUsers :many
-- List all users ordered by username
SELECT id, username, email, created_at
FROM users
ORDER BY username;

-- name: ListActiveUsers :many
-- List users who are active using named parameters
SELECT id, username, email
FROM users
WHERE is_active = @is_active AND created_at > @since
ORDER BY created_at DESC;

-- name: CreateUser :one
-- Create a new user and return the created record
INSERT INTO users (username, email, bio)
VALUES (@username, @email, @bio)
RETURNING id, username, email, created_at;

-- name: UpdateUser :exec
-- Update an existing user
UPDATE users
SET
    username = @username,
    email = @email,
    updated_at = NOW()
WHERE id = @user_id;

-- name: UpdateUserBio :exec
-- Update only the bio field
UPDATE users
SET bio = @bio, updated_at = NOW()
WHERE id = @user_id;

-- name: DeleteUser :execrows
-- Delete a user and return the number of rows affected
DELETE FROM users
WHERE id = @user_id;

-- name: DeleteInactiveUsers :execrows
-- Delete users who haven't been active
DELETE FROM users
WHERE is_active = false AND last_login_at < @cutoff_date;

-- name: CountUsers :one
-- Count total number of users
SELECT COUNT(*) as total
FROM users;

-- name: SearchUsers :many
-- Search users by username pattern with limit
SELECT id, username, email
FROM users
WHERE username ILIKE @search_pattern
ORDER BY username
LIMIT @limit_count;

-- name: GetUserWithPosts :many
-- Test LEFT JOIN nullability analysis
SELECT
    u.id as user_id,
    u.username,
    p.id as post_id,
    p.title,
    p.content
FROM users u
LEFT JOIN posts p ON u.id = p.user_id
WHERE u.id = @user_id;

-- name: GetUserStats :one
-- Test aggregate functions with positional parameters
SELECT
    COUNT(*) as post_count,
    COALESCE(MAX(p.created_at), NOW()) as last_post_at
FROM posts p
WHERE p.user_id = $1;
