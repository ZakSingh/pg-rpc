SELECT nullable_int IS NULL as test FROM (VALUES (NULL)) t(nullable_int);
EOF < /dev/null