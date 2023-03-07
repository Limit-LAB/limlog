# LIMLOG

High-performance log storage system for storing sequential messages and providing query functions, 64bit optimized.

Two log formats are available:

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

| Field             | Size           |
| ----------------- | -------------- |
| uuid              | 16 bytes       |
| body_len (u64 LE) | 8 bytes        |
| body              | body_len bytes |

### .idx

- header

| Field        | Size    |
| ------------ | ------- |
| magic_number | 8 bytes |
| attributes   | 8 bytes |

- index item

| Field           | Size     |
| --------------- | -------- |
| uuid            | 16 bytes |
| offset (u64 LE) | 8 bytes  |
