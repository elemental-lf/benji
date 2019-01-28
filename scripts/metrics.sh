#!/usr/bin/env bash
io::prometheus::NewGauge name=benji_command_start_time labels=command,auxiliary_data,arguments help='Start time of Benji command (time_t)'
io::prometheus::NewGauge name=benji_command_completion_time labels=command,auxiliary_data,arguments help='Completion time of Benji command (time_t)'
io::prometheus::NewGauge name=benji_command_runtime_seconds labels=command,auxiliary_data,arguments help='Runtime of Benji command (seconds)'
io::prometheus::NewGauge name=benji_command_status_succeeded labels=command,auxiliary_data,arguments help='Benji command succeeded'
io::prometheus::NewGauge name=benji_command_status_failed labels=command,auxiliary_data,arguments help='Benji command failed'

io::prometheus::NewGauge name=benji_backup_start_time labels=command,auxiliary_data,version_name help='Start time of Benji backup command (time_t)'
io::prometheus::NewGauge name=benji_backup_completion_time labels=command,auxiliary_data,version_name help='Completion time of Benji backup command (time_t)'
io::prometheus::NewGauge name=benji_backup_runtime_seconds labels=command,auxiliary_data,version_name help='Runtime of Benji backup command (seconds)'
io::prometheus::NewGauge name=benji_backup_status_succeeded labels=command,auxiliary_data,version_name help='Benji backup command succeeded'
io::prometheus::NewGauge name=benji_backup_status_failed labels=command,auxiliary_data,version_name help='Benji backup command  failed'

io::prometheus::NewGauge name=benji_invalid_versions help='Number of invalid backup versions'
io::prometheus::NewGauge name=benji_older_incomplete_versions help='Number of older incomplete versions'
