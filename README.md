## MySQL8

This is the MySQL shared branch of kunpengcompute. The mysql-8.0.20-oltp-kp branch is optimized for ARM based on the [mysql-8.0.20](https://github.com/mysql/mysql-server/tree/mysql-8.0.20). "8.0" and "5.7" are branches released by Oracle. 

Opt-in features will be provided as patch files in the patch directory. Currently the patch directory includes 3 patch files:
1. The 0001-SHARDED-LOCK-SYS.patch
The Lock-sys orchestrates access to tables and rows. Lock-sys stores both GRANTED and WAITING lock requests in lists known as queues. In the past a single latch protected access to all of these queues. In this patch, a more granular approach to latching is used to latch these queues in safe and quick fashion.

2. The 0001-SHARDED-LOCK-SYS-AND-LOCKFREE-TRX-SYS.patch
The Trx-sys is the transaction system central memory data structure. In the past a single latch protected most fields in this structure. In this patch, a lock-free hash is used to optimize a key field named rw_trx_ids, which frees the readview management from the latch competition. The SHARDED-LOCK-SYS feature is also included in this patch.

3. The 0001-CRC32-ARM.patch
In this patch, a crc32 function accelerated by ARM specific instrument is provided. Scenarios in ARM with binlog enabled will benefit from this.

## License

This project is licensed by the GPL-2.0 authorization agreement in the [MySQL 8 LICENSE](https://github.com/mysql/mysql-server/blob/8.0/LICENSE) file. For details about the agreement, see the LICENSE file in the project.
