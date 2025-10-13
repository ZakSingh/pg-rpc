-- Example SQL query file for pgrpc
-- Demonstrates the sqlc-like query generation feature

-- name: GetAuthor :one
-- Fetch a single author by ID
SELECT id, name, bio, created_at
FROM authors
WHERE id = @author_id
LIMIT 1;

-- name: GetAuthorByEmail :one
-- Fetch author by email address
SELECT id, name, bio, email, created_at
FROM authors
WHERE email = @email;

-- name: ListAuthors :many
-- List all authors ordered by name
SELECT id, name, bio, created_at
FROM authors
ORDER BY name;

-- name: ListAuthorsByCreatedDate :many
-- List authors created after a specific date
SELECT id, name, bio, created_at
FROM authors
WHERE created_at > @since
ORDER BY created_at DESC;

-- name: CreateAuthor :one
-- Create a new author and return the created record
INSERT INTO authors (name, bio, email)
VALUES (@name, @bio, @email)
RETURNING id, name, bio, email, created_at;

-- name: UpdateAuthor :exec
-- Update an existing author
UPDATE authors
SET
    name = @name,
    bio = @bio,
    updated_at = NOW()
WHERE id = @author_id;

-- name: UpdateAuthorEmail :exec
-- Update only the email address
UPDATE authors
SET email = @email, updated_at = NOW()
WHERE id = @author_id;

-- name: DeleteAuthor :execrows
-- Delete an author and return the number of rows affected
DELETE FROM authors
WHERE id = @author_id;

-- name: DeleteInactiveAuthors :execrows
-- Delete authors who haven't been active
DELETE FROM authors
WHERE last_active_at < @cutoff_date;

-- name: CountAuthors :one
-- Count total number of authors
SELECT COUNT(*) as total
FROM authors;

-- name: SearchAuthors :many
-- Search authors by name pattern
SELECT id, name, bio
FROM authors
WHERE name ILIKE @search_pattern
ORDER BY name
LIMIT @limit_count;
