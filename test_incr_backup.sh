#!/usr/bin/env bash
# test_incr_backup.sh
# PostgreSQL 17 増分バックアップのテスト用ベンチマークスクリプト

set -euo pipefail

PG_BIN="/lib/postgresql/17/bin"
PG_PORT=16432
DB_USER="postgres"
BENCH_DB="benchdb"
PGDATA="/var/lib/postgresql/17/main"
BACKUP_BASE_DIR="/tmp/pgbackup_test"
SCALES=(10 20)
TRANSACTIONS=(500 1000)
INCREMENTAL_COUNT=2
COMPRESS_OPTION="--compress=server-zstd"
MON_INTERVAL=1

mkdir -p "$BACKUP_BASE_DIR"


start_resource_monitoring() {
  local prefix="$1"
  pidstat -urd -h $MON_INTERVAL > "${prefix}_pidstat.log" 2>&1 &
  PIDSTAT_PID=$!
  iostat -xm $MON_INTERVAL > "${prefix}_iostat.log" 2>&1 &
  IOSTAT_PID=$!
}

stop_resource_monitoring() {
  kill $PIDSTAT_PID || true
  kill $IOSTAT_PID || true
}

full_backup() {
  local backup_dir="$1"
  local log_prefix="$2"

  echo "=== Full backup => $backup_dir ==="
  start_resource_monitoring "${log_prefix}_fullbackup"

  /usr/bin/time -v "$PG_BIN/pg_basebackup" \
    -p "$PG_PORT" --format=tar --wal-method=stream \
    --checkpoint=fast --manifest-checksums=CRC32C \
    $COMPRESS_OPTION \
    -D "$backup_dir" \
    > "${log_prefix}_fullbackup_time.log" 2>&1

  stop_resource_monitoring

  du -sh "$backup_dir" > "${log_prefix}_fullbackup_size.log"
}

incremental_backup() {
  local prev_manifest="$1"
  local backup_dir="$2"
  local log_prefix="$3"

  echo "=== Incremental backup => $backup_dir ==="
  start_resource_monitoring "${log_prefix}_incbackup"

  /usr/bin/time -v "$PG_BIN/pg_basebackup" \
    -p "$PG_PORT" --format=tar --wal-method=stream \
    --checkpoint=fast --manifest-checksums=CRC32C \
    --incremental="$prev_manifest" \
    $COMPRESS_OPTION \
    -D "$backup_dir" \
    > "${log_prefix}_incbackup_time.log" 2>&1

  stop_resource_monitoring

  du -sh "$backup_dir" > "${log_prefix}_incbackup_size.log"
}

pgbench_init() {
  local scale="$1"
  dropdb -p "$PG_PORT" -U "$DB_USER" --if-exists "$BENCH_DB"
  createdb -p "$PG_PORT" -U "$DB_USER" "$BENCH_DB"
  "$PG_BIN/pgbench" -p "$PG_PORT" -U "$DB_USER" -i -s "$scale" "$BENCH_DB"
}

pgbench_run() {
  local txcount="$1"
  local log_prefix="$2"

  /usr/bin/time -v "$PG_BIN/pgbench" \
    -p "$PG_PORT" -c 10 -j 2 -t "$txcount" "$BENCH_DB" \
    > "${log_prefix}_pgbench_t${txcount}.log" 2>&1
}

prepare_backup_for_combine() {
  local tar_dir="$1"
  local out_dir="$2"
  mkdir -p "$out_dir"
  tar -I 'zstd -d' -xf "$tar_dir/base.tar.zst" -C "$out_dir"
  cp "$tar_dir/backup_manifest" "$out_dir"
  mkdir -p "$out_dir/pg_wal"
  tar -xf "$tar_dir/pg_wal.tar" -C "$out_dir/pg_wal"
}

___combine_backup_test() {
  local prepared_dirs=("$@")
  local output_dir="${prepared_dirs[-1]}/../combined"
  local base_log_dir
  base_log_dir=$(dirname "${prepared_dirs[0]}")

  mkdir -p "$output_dir"
  echo "=== Combine backup => $output_dir ==="

  start_resource_monitoring "${base_log_dir}/combine_backup"

  /usr/bin/time -v "$PG_BIN/pg_combinebackup" --verbose \
    -o "$output_dir" "${prepared_dirs[@]}" \
    > "${base_log_dir}/combine_backup_time.log" 2>&1

  stop_resource_monitoring

  du -sh "$output_dir" > "${base_log_dir}/combine_backup_size.log"
}
combine_backup_test() {
  local prepared_dirs=("$@")
  local output_dir="$(dirname ${prepared_dirs[0]})/combined"

  mkdir -p "$output_dir"
  echo "=== Combine backup => $output_dir ==="

  start_resource_monitoring "${output_dir}/../logs/combine_backup"

  /usr/bin/time -v "$PG_BIN/pg_combinebackup" \
    "${prepared_dirs[@]}" -o "$output_dir" \
    > "$(dirname ${prepared_dirs[0]})/logs/combine_backup_time.log" 2>&1

  stop_resource_monitoring

  du -sh "$output_dir" > "${output_dir}/../logs/combine_backup_size.log"
  $PG_BIN/pg_verifybackup "$output_dir"
}


echo "== PostgreSQL 17 Incremental Backup Benchmark =="
echo "Logs => $BACKUP_BASE_DIR"

for scale in "${SCALES[@]}"; do
  for txcount in "${TRANSACTIONS[@]}"; do
    SCENARIO_NAME="scale${scale}_tx${txcount}"
    SCENARIO_DIR="$BACKUP_BASE_DIR/$SCENARIO_NAME"
    mkdir -p "$SCENARIO_DIR/logs"

    echo "=== Scenario: $SCENARIO_NAME ==="

    pgbench_init "$scale"

    FULL_BKP_DIR="$SCENARIO_DIR/full_base"
    full_backup "$FULL_BKP_DIR" "$SCENARIO_DIR/logs/$SCENARIO_NAME"

    FULL_TIME=$(grep "Elapsed" "$SCENARIO_DIR/logs/${SCENARIO_NAME}_fullbackup_time.log" | awk '{print $8}')
    FULL_SIZE=$(awk '{print $1}' "$SCENARIO_DIR/logs/${SCENARIO_NAME}_fullbackup_size.log")
    PREV_MANIFEST="$FULL_BKP_DIR/backup_manifest"

    for (( i=1; i<=$INCREMENTAL_COUNT; i++ )); do
      pgbench_run "$txcount" "$SCENARIO_DIR/logs/${SCENARIO_NAME}_inc${i}"

      INC_BKP_DIR="$SCENARIO_DIR/inc${i}"
      incremental_backup "$PREV_MANIFEST" "$INC_BKP_DIR" "$SCENARIO_DIR/logs/${SCENARIO_NAME}_inc${i}"

      PREV_MANIFEST="$INC_BKP_DIR/backup_manifest"
    done

    FULL_PREPARED="$SCENARIO_DIR/full_base_prepared"
    prepare_backup_for_combine "$FULL_BKP_DIR" "$FULL_PREPARED"
    PREPARED_DIRS=("$FULL_PREPARED")

    for (( i=1; i<=$INCREMENTAL_COUNT; i++ )); do
      incprep="$SCENARIO_DIR/inc${i}_prepared"
      prepare_backup_for_combine "$SCENARIO_DIR/inc${i}" "$incprep"
      PREPARED_DIRS+=("$incprep")
    done

    combine_backup_test "${PREPARED_DIRS[@]}"

    echo "=== Scenario $SCENARIO_NAME done ==="
  done
done

echo "== All Scenarios Completed =="
