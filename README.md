# LIMLOG

高性能日志存储系统，用于存储顺序消息并提供查询功能
64bit optimized

提供两种日志格式:

1. `<start_uuid>.limlog` `<start_uuid>.idx` 用于存储顺序消息
2. TODO

## Files Format

### .limlog

- header

| Field        | Size    |
| ------------ | ------- |
| magic_number | 8 bytes |
| attributes   | 8 bytes |

- log

| Field              | Size            |
| ------------------ | --------------- |
| uuid               | 16 bytes        |
| key_len (u64 LE)   | 8 bytes         |
| key                | key_len bytes   |
| value_len (u64 LE) | 8 bytes         |
| value              | value_len bytes |

### .idx

- header

| Field        | Size    |
| ------------ | ------- |
| magic_number | 8 bytes |
| attributes   | 8 bytes |

- index item

| Field               | Size     |
| ------------------- | -------- |
| uuid                | 16 bytes |
| offset (u64 LE)     | 8 bytes  |
