# LIMLOG

高性能日志存储系统，用于存储顺序的消息并提供查询功能
64bit optimized

提供两种日志格式:

1. `<start_id>_<start_ts>.limlog` `<start_id>_<start_ts>.idx` `<start_id>_<start_ts>.ts.idx` 用于存储顺序的消息
2. TODO

## Files Format

### .limlog

- header

| Field        | Size    |
| ------------ | ------- |
| magic_number | 8 bytes |
| attributes   | 8 bytes |
| entry_count  | 8 bytes |

- log

| Field         | Size              |
| ------------- | ----------------- |
| ts(Timestamp) | 8 bytes           |
| id            | 8 bytes           |
| __key_len     | 8 bytes           |
| key           | __key_len bytes   |
| __value_len   | 8 bytes           |
| value         | __value_len bytes |

### .idx

- header

| Field        | Size    |
| ------------ | ------- |
| magic_number | 8 bytes |

- index item

| Field                 | Size    |
| --------------------- | ------- |
| __id                  | 8 bytes |
| __offset (of .limlog) | 8 bytes |

### .ts.idx

- header

| Field        | Size    |
| ------------ | ------- |
| magic_number | 8 bytes |

- index item

| Field                 | Size    |
| --------------------- | ------- |
| __ts                  | 8 bytes |
| __offset (of .limlog) | 8 bytes |
