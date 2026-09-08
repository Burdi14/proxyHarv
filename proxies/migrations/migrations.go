// Package migrations embeds the SQL migrations under migrations/.
package migrations

import (
	"embed"
	"io/fs"
	"sort"
)

//go:embed *.sql
var files embed.FS

// FS exposes the embedded migrations filesystem.
func FS() fs.FS { return files }

// Files returns migration files sorted by name (0001_…, 0002_…, …).
func Files() []fs.File {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil
	}
	names := []string{}
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".sql" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	out := []fs.File{}
	for _, n := range names {
		if f, err := files.Open(n); err == nil {
			out = append(out, f)
		}
	}
	return out
}

// Names returns the sorted migration file names (for tooling/tests).
func Names() []string {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil
	}
	names := []string{}
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".sql" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}
