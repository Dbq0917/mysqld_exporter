// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestInstanceReconnectKeepsOwnConnectionSettings(t *testing.T) {
	baseDSN := os.Getenv("TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("TEST_MYSQL_DSN is not set")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	adminConfig, err := mysql.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	adminConfig.DBName = ""
	admin, err := sql.Open("mysql", adminConfig.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = admin.Close()
	})

	suffix := fmt.Sprintf("mysqld_exporter_multi_%d", os.Getpid())
	firstDB := suffix + "_first"
	secondDB := suffix + "_second"
	for _, database := range []string{firstDB, secondDB} {
		if _, err := admin.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+database+"`"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			_, _ = admin.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+database+"`")
		})
	}

	dsnFor := func(database string, lockWaitTimeout int) string {
		cfg, err := mysql.ParseDSN(baseDSN)
		if err != nil {
			t.Fatal(err)
		}
		cfg.DBName = database
		if cfg.Params == nil {
			cfg.Params = make(map[string]string)
		}
		cfg.Params["lock_wait_timeout"] = strconv.Itoa(lockWaitTimeout)
		return cfg.FormatDSN()
	}

	const instanceCount = 20
	instances := make([]*instance, 0, instanceCount)
	for i := range instanceCount {
		database := firstDB
		if i%2 == 1 {
			database = secondDB
		}
		inst, err := newInstance(ctx, dsnFor(database, i+1), 2, 1, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer inst.Close()
		instances = append(instances, inst)
	}

	assertConnectionSettings := func(inst *instance, wantDatabase string, wantTimeout int) {
		t.Helper()

		var (
			database string
			timeout  int
		)
		if err := inst.getDB().QueryRowContext(ctx, "SELECT DATABASE(), @@SESSION.lock_wait_timeout").Scan(&database, &timeout); err != nil {
			t.Fatal(err)
		}
		if database != wantDatabase || timeout != wantTimeout {
			t.Fatalf("connection settings = (%q, %d), want (%q, %d)", database, timeout, wantDatabase, wantTimeout)
		}
	}

	assertAll := func() {
		t.Helper()
		for i, inst := range instances {
			database := firstDB
			if i%2 == 1 {
				database = secondDB
			}
			assertConnectionSettings(inst, database, i+1)
		}
	}

	assertAll()

	const reconnectIndex = 7
	killCurrentConnection(t, ctx, admin, instances[reconnectIndex])

	if err := instances[reconnectIndex].reconnect(ctx); err != nil {
		t.Fatalf("reconnect instance %d: %v", reconnectIndex, err)
	}

	const pingReconnectIndex = 8
	if err := instances[pingReconnectIndex].getDB().Close(); err != nil {
		t.Fatal(err)
	}
	if err := instances[pingReconnectIndex].Ping(ctx); err != nil {
		t.Fatalf("ping reconnect instance %d: %v", pingReconnectIndex, err)
	}

	assertAll()
}

func killCurrentConnection(t *testing.T, ctx context.Context, admin *sql.DB, inst *instance) {
	t.Helper()

	conn, err := inst.getDB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var connectionID int64
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, fmt.Sprintf("KILL CONNECTION %d", connectionID)); err != nil {
		t.Fatal(err)
	}
}
