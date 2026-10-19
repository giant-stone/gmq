package gmq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/giant-stone/gmq/schemas"
	"github.com/google/uuid"
	_ "modernc.org/sqlite" // Register the pure Go SQLite driver. // cspell:ignore modernc
)

const (
	sqliteBusyTimeout   = 5 * time.Second
	sqliteSchemaVersion = 1
)

// BrokerSQLite stores queues in a SQLite database without requiring CGO.
type BrokerSQLite struct {
	db        *sql.DB
	clockLock sync.RWMutex
	clock     Clock
	utc       bool
}

var _ Broker = (*BrokerSQLite)(nil)

// NewBrokerSQLite opens a filename or SQLite file URI. Use :memory: for an
// isolated, temporary broker. Close the broker when it is no longer needed.
func NewBrokerSQLite(dsn string) (Broker, error) {
	if dsn == "" {
		return nil, errors.New("SQLite database filename is required")
	}
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	pragmas := url.Values{}
	pragmas.Set("_txlock", "immediate") // cspell:ignore txlock
	pragmas.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", sqliteBusyTimeout.Milliseconds()))
	pragmas.Add("_pragma", "foreign_keys(1)")
	pragmas.Add("_pragma", "journal_mode(WAL)")
	db, err := sql.Open("sqlite", dsn+separator+pragmas.Encode())
	if err != nil {
		return nil, err
	}
	// One connection also keeps :memory: databases alive and serializes local writes.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	broker := &BrokerSQLite{db: db, clock: NewWallClock()}
	ctx, cancel := context.WithTimeout(context.Background(), sqliteBusyTimeout)
	defer cancel()
	if err = broker.createSchema(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize SQLite broker: %w", err)
	}
	return broker, nil
}

func (it *BrokerSQLite) createSchema(ctx context.Context) error {
	tx, err := it.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version > sqliteSchemaVersion {
		return fmt.Errorf("SQLite schema version %d exceeds supported version %d", version, sqliteSchemaVersion)
	}
	if version == sqliteSchemaVersion {
		return tx.Commit()
	}
	for _, name := range []string{"queues", "messages", "failures", "daily_stats"} {
		definition, err := schemas.Files.ReadFile("migrations/26-10-19_" + name + "_create.sql")
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, string(definition)); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", sqliteSchemaVersion)); err != nil {
		return err
	}
	return tx.Commit()
}

func (it *BrokerSQLite) Close() error                   { return it.db.Close() }
func (it *BrokerSQLite) Ping(ctx context.Context) error { return it.db.PingContext(ctx) }

func (it *BrokerSQLite) SetClock(clock Clock) {
	it.clockLock.Lock()
	defer it.clockLock.Unlock()
	it.clock = clock
}

func (it *BrokerSQLite) UTC(flag bool) {
	it.clockLock.Lock()
	defer it.clockLock.Unlock()
	it.utc = flag
}

func (it *BrokerSQLite) getNow() time.Time {
	it.clockLock.RLock()
	defer it.clockLock.RUnlock()
	now := it.clock.Now()
	if it.utc {
		return now.UTC()
	}
	return now.Local()
}

func (it *BrokerSQLite) Init(ctx context.Context, queueName string) error {
	_, err := it.db.ExecContext(ctx, "INSERT INTO queues(name) VALUES (?) ON CONFLICT DO NOTHING", queueName)
	return err
}

func (it *BrokerSQLite) Pause(ctx context.Context, queueName string) error {
	_, err := it.db.ExecContext(ctx, `INSERT INTO queues(name, paused) VALUES (?, 1)
		ON CONFLICT(name) DO UPDATE SET paused = 1`, queueName)
	return err
}

func (it *BrokerSQLite) Resume(ctx context.Context, queueName string) error {
	_, err := it.db.ExecContext(ctx, "UPDATE queues SET paused = 0 WHERE name = ?", queueName)
	return err
}

func (it *BrokerSQLite) Enqueue(ctx context.Context, msg IMsg, opts ...OptionClient) (*Msg, error) {
	queue, id, uniqueFor := msg.GetQueue(), msg.GetId(), DefaultTTLMsgUniq
	for _, opt := range opts {
		switch opt.Type() {
		case OptTypeQueueName:
			if name := opt.Value().(string); name != "" {
				queue = name
			}
		case OptTypeUniqueIn:
			if duration := opt.Value().(time.Duration); duration.Milliseconds() > 0 {
				uniqueFor = duration
			}
		}
	}
	if queue == "" {
		queue = DefaultQueueName
	}
	if id == "" {
		id = uuid.NewString()
	}
	now := it.getNow()
	result := &Msg{Queue: queue, Id: id, Payload: msg.GetPayload(), State: MsgStatePending,
		Created: now.UnixMilli(), Updated: now.UnixMilli(), ExpiresAt: now.Add(uniqueFor).UnixMilli()}
	tx, err := it.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO queues(name) VALUES (?) ON CONFLICT DO NOTHING", queue); err != nil {
		return nil, err
	}
	// Keep completed reservations until their uniqueness deadline, without retaining payloads.
	if _, err = tx.ExecContext(ctx, "DELETE FROM messages WHERE state = '' AND expires_at <= ?", now.UnixMilli()); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM messages WHERE queue = ? AND id = ? AND expires_at <= ?", queue, id, now.UnixMilli()); err != nil {
		return nil, err
	}
	insert, err := tx.ExecContext(ctx, `INSERT INTO messages(queue, id, payload, created, expires_at, updated, state)
		VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(queue, id) DO NOTHING`, queue, id, result.Payload,
		result.Created, result.ExpiresAt, result.Updated, result.State)
	if err != nil {
		return nil, err
	}
	count, err := insert.RowsAffected()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, ErrMsgIdConflict
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

const sqliteMessageColumns = "id, queue, payload, created, expires_at, updated, state"

func scanSQLiteMessage(row interface{ Scan(...any) error }) (*Msg, error) {
	msg := &Msg{}
	err := row.Scan(&msg.Id, &msg.Queue, &msg.Payload, &msg.Created, &msg.ExpiresAt, &msg.Updated, &msg.State)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoMsg
	}
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (it *BrokerSQLite) Dequeue(ctx context.Context, queueName string) (*Msg, error) {
	// A single write statement claims the oldest message across all broker instances.
	return scanSQLiteMessage(it.db.QueryRowContext(ctx, `UPDATE messages SET state = ?, updated = ?
		WHERE sequence = (SELECT m.sequence FROM messages m JOIN queues q ON q.name = m.queue
		WHERE m.queue = ? AND m.state = ? AND q.paused = 0 ORDER BY m.sequence LIMIT 1)
		RETURNING `+sqliteMessageColumns, MsgStateProcessing, it.getNow().UnixMilli(), queueName, MsgStatePending))
}

func (it *BrokerSQLite) GetMsg(ctx context.Context, queueName, id string) (*Msg, error) {
	return scanSQLiteMessage(it.db.QueryRowContext(ctx, "SELECT "+sqliteMessageColumns+` FROM messages
		WHERE queue = ? AND id = ? AND state IN (?, ?) AND expires_at > ?`,
		queueName, id, MsgStatePending, MsgStateProcessing, it.getNow().UnixMilli()))
}

func (it *BrokerSQLite) Complete(ctx context.Context, msg IMsg) error {
	return it.finish(ctx, msg, nil)
}

func (it *BrokerSQLite) Fail(ctx context.Context, msg IMsg, failure error) error {
	if failure == nil {
		return errors.New("failure error is required")
	}
	return it.finish(ctx, msg, failure)
}

func (it *BrokerSQLite) finish(ctx context.Context, msg IMsg, failure error) error {
	tx, err := it.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := it.getNow()
	state := ""
	if failure != nil {
		state = MsgStateFailed
	}
	stored, err := scanSQLiteMessage(tx.QueryRowContext(ctx, `UPDATE messages SET state = ?, updated = ?
		WHERE queue = ? AND id = ? AND state IN (?, ?) RETURNING `+sqliteMessageColumns,
		state, now.UnixMilli(), msg.GetQueue(), msg.GetId(), MsgStatePending, MsgStateProcessing))
	if err != nil {
		return err
	}
	completed, failed := 1, 0
	if failure != nil {
		completed, failed = 0, 1
		if err = insertSQLiteFailure(ctx, tx, stored, failure); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE messages SET payload = NULL WHERE queue = ? AND id = ?", stored.Queue, stored.Id); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO daily_stats(queue, date, completed, failed) VALUES (?, ?, ?, ?)
		ON CONFLICT(queue, date) DO UPDATE SET completed = completed + excluded.completed, failed = failed + excluded.failed`,
		stored.Queue, now.Format("2006-01-02"), completed, failed)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func insertSQLiteFailure(ctx context.Context, tx *sql.Tx, msg *Msg, failure error) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO failures(queue, id, payload, created, expires_at, updated, error)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, msg.Queue, msg.Id, msg.Payload, msg.Created, msg.ExpiresAt, msg.Updated, failure.Error())
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM failures WHERE queue = ? AND id = ? AND sequence NOT IN
		(SELECT sequence FROM failures WHERE queue = ? AND id = ? ORDER BY sequence DESC LIMIT ?)`,
		msg.Queue, msg.Id, msg.Queue, msg.Id, DefaultMaxItemsLimit)
	return err
}

func (it *BrokerSQLite) DeleteQueue(ctx context.Context, queueName string) error {
	_, err := it.db.ExecContext(ctx, "DELETE FROM queues WHERE name = ?", queueName)
	return err
}

func (it *BrokerSQLite) DeleteMsg(ctx context.Context, queueName, id string) error {
	return it.deleteMessages(ctx, queueName, "id = ?", id)
}

func (it *BrokerSQLite) DeleteAgo(ctx context.Context, queueName string, duration time.Duration) error {
	return it.deleteMessages(ctx, queueName, "created <= ?", it.getNow().Add(-duration).UnixMilli())
}

func (it *BrokerSQLite) deleteMessages(ctx context.Context, queueName, predicate string, value any) error {
	tx, err := it.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, table := range []string{"messages", "failures"} {
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE queue = ? AND "+predicate, queueName, value); err != nil {
			return err
		}
	}
	return tx.Commit()
}
