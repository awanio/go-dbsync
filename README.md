# go-dbsync
A Go program that syncs updated records between two database instances using GORM, supporting both MySQL and PostgreSQL

Build:

```bash
go build -o go-dbsync
```

How to run:

```bash
go run go-dbsync \
  --source-db-host=localhost \
  --source-db-user=user1 \
  --source-db-name=db1 \
  --source-db-password=pass1 \
  --target-db-host=remote-server \
  --target-db-user=user2 \
  --target-db-name=db2 \
  --target-db-password=pass2 \
  --db-vendor=mysql \
  --updated-at-date=2025-02-01 \
  --exclude-tables="users,logs" \
  --exclude-columns="created_at,deleted_at,some_column"
  ```