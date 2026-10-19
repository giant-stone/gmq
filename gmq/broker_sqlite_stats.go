package gmq

import (
	"context"
	"strings"
)

func sqlitePageLimit(limit int64) int64 {
	if limit <= 0 {
		return -1
	}
	return limit
}

func (it *BrokerSQLite) ListQueue(ctx context.Context) ([]string, error) {
	return it.queryStrings(ctx, "SELECT name FROM queues ORDER BY name")
}

func (it *BrokerSQLite) ListMsg(ctx context.Context, queueName, state string, limit, offset int64) ([]string, error) {
	if state == MsgStateFailed {
		return it.queryStrings(ctx, `SELECT id FROM failures WHERE queue = ? GROUP BY id
			ORDER BY MAX(sequence) LIMIT ? OFFSET ?`, queueName, sqlitePageLimit(limit), offset)
	}
	if state != MsgStatePending && state != MsgStateProcessing {
		return []string{}, nil
	}
	return it.queryStrings(ctx, `SELECT id FROM messages WHERE queue = ? AND state = ?
		ORDER BY sequence LIMIT ? OFFSET ?`, queueName, state, sqlitePageLimit(limit), offset)
}

func (it *BrokerSQLite) queryStrings(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := it.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]string, 0)
	for rows.Next() {
		var value string
		if err = rows.Scan(&value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func (it *BrokerSQLite) ListFailed(ctx context.Context, queueName, id string, limit, offset int64) ([]*Msg, error) {
	rows, err := it.db.QueryContext(ctx, `SELECT id, queue, payload, created, expires_at, updated, error
		FROM failures WHERE queue = ? AND id = ? ORDER BY sequence DESC LIMIT ? OFFSET ?`,
		queueName, id, sqlitePageLimit(limit), offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]*Msg, 0)
	for rows.Next() {
		msg := &Msg{State: MsgStateFailed}
		if err = rows.Scan(&msg.Id, &msg.Queue, &msg.Payload, &msg.Created, &msg.ExpiresAt, &msg.Updated, &msg.Err); err != nil {
			return nil, err
		}
		result = append(result, msg)
	}
	return result, rows.Err()
}

func (it *BrokerSQLite) GetStats(ctx context.Context) ([]*QueueStat, error) {
	rows, err := it.db.QueryContext(ctx, `SELECT q.name,
		(SELECT COUNT(*) FROM messages WHERE queue = q.name AND state = ?),
		(SELECT COUNT(*) FROM messages WHERE queue = q.name AND state = ?),
		(SELECT COUNT(DISTINCT id) FROM failures WHERE queue = q.name)
		FROM queues q ORDER BY q.name`, MsgStatePending, MsgStateProcessing)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]*QueueStat, 0)
	for rows.Next() {
		stat := &QueueStat{}
		if err = rows.Scan(&stat.Name, &stat.Pending, &stat.Processing, &stat.Failed); err != nil {
			return nil, err
		}
		stat.Total = stat.Pending + stat.Processing + stat.Failed
		result = append(result, stat)
	}
	return result, rows.Err()
}

func (it *BrokerSQLite) GetStatsByDate(ctx context.Context, queues []string, date string) (*QueueDailyStat, error) {
	result := &QueueDailyStat{Date: date}
	if len(queues) == 0 {
		return result, ctx.Err()
	}
	args := make([]any, 0, len(queues)+1)
	args = append(args, date)
	for _, queue := range queues {
		args = append(args, queue)
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(queues)), ",")
	err := it.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(completed), 0), COALESCE(SUM(failed), 0)
		FROM daily_stats WHERE date = ? AND queue IN (`+placeholders+")", args...).Scan(&result.Completed, &result.Failed)
	if err != nil {
		return nil, err
	}
	result.Total = result.Completed + result.Failed
	return result, nil
}

func (it *BrokerSQLite) GetStatsWeekly(ctx context.Context, queues []string) ([]*QueueDailyStat, error) {
	// Match the Redis broker: seven preceding days plus today, oldest first.
	const precedingDays = 7
	date := it.getNow().AddDate(0, 0, -precedingDays)
	result := make([]*QueueDailyStat, 0, precedingDays+1)
	for day := 0; day <= precedingDays; day++ {
		stat, err := it.GetStatsByDate(ctx, queues, date.Format("2006-01-02"))
		if err != nil {
			return nil, err
		}
		result = append(result, stat)
		date = date.AddDate(0, 0, 1)
	}
	return result, nil
}
