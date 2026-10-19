// Package schemas embeds the SQLite broker schema and explicit migrations.
package schemas

import "embed"

// Files contains table definitions and ordered migration records.
//
//go:embed *.sql migrations/*.sql
var Files embed.FS
