package plugin

import (
	"context"
	"log"
	"path"

	"github.com/fsnotify/fsnotify"
	filehelpers "github.com/turbot/go-kit/files"
	"github.com/turbot/go-kit/filewatcher"
	"github.com/turbot/steampipe-plugin-sdk/v5/getter"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// ConnectionData is the data stored by the plugin which is connection dependent.
type ConnectionData struct {
	// map of all the tables in the plugin, keyed by the table name
	TableMap   map[string]*Table
	Connection *Connection
	// schema may be connection specific for dynamic schemas
	Schema map[string]*proto.TableSchema
	// this is connection specific filewatcher to watch for the changes in files (if needed)
	Watcher *filewatcher.FileWatcher
	// map of file paths which will update the schema if changed
	SchemaChangingWatchPaths []string
	Plugin                   *Plugin
}

// GetConnectionTempDir appends the connection name to the plugin temporary directory path
func (d *ConnectionData) GetConnectionTempDir(pluginTempDir string) string {
	return path.Join(pluginTempDir, d.Connection.Name)
}

func (d *ConnectionData) updateWatchPaths(watchPaths []watchedPath, p *Plugin) error {
	// close any existing watcher
	if d.Watcher != nil {
		log.Printf("[TRACE] ConnectionData updateWatchPaths - close existing watcher")
		d.Watcher.Close()
		d.Watcher = nil
	}

	// create WatcherOptions
	connTempDir := d.GetConnectionTempDir(p.tempDir)
	opts := filewatcher.WatcherOptions{
		EventMask: fsnotify.Create | fsnotify.Write | fsnotify.Remove | fsnotify.Rename,
	}

	// Iterate through watch paths to resolve and
	// add resolved paths to file watcher options
	log.Printf("[TRACE] ConnectionData.updateWatchPaths - create watcher options from the watchPaths %v", watchPaths)
	for _, path := range watchPaths {
		dest, globPattern, err := getter.GetFiles(path.watchPath, connTempDir)
		if err != nil {
			log.Printf("[WARN] ConnectionData updateWatchPaths - error resolving source path %s: %s", path.watchPath, err.Error())
			continue
		}
		opts.Directories = append(opts.Directories, dest)
		opts.Include = append(opts.Include, globPattern)
		// if this path alters schema, add to SchemaChangingWatchPaths
		if path.altersSchema {
			d.SchemaChangingWatchPaths = append(d.SchemaChangingWatchPaths, globPattern)
		}
	}

	// if we have no paths, do not start a watcher
	if len(opts.Directories) == 0 {
		log.Printf("[WARN] ConnectionData updateWatchPaths - no watch paths resolved - not creating watcher")
		return nil
	}
	// Add the callback function for the filewatchers to watcher options
	opts.OnChange = func(events []fsnotify.Event) {
		p.WatchedFileChangedFunc(context.Background(), p, d.Connection, events)

		log.Printf("[WARN] watched connection files changed")

		// for each event, check whether it affects the schema
		if d.fileChangesUpdateSchema(events) {
			log.Printf("[WARN] watched connection files updated schema")
			if err := d.Plugin.ConnectionSchemaChanged(d.Connection); err != nil {
				log.Printf("[WARN] failed to update plugin schema after file event: %s", err.Error())
			}
		}
	}

	// Get the new file watcher from file options
	newWatcher, err := filewatcher.NewWatcher(&opts)
	if err != nil {
		log.Printf("[WARN] ConnectionData.updateWatchPaths - failed to create a new file watcher: %s", err.Error())
		return err
	}
	log.Printf("[TRACE] ConnectionData.updateWatchPaths - created the new file watcher")

	// Start new watcher
	newWatcher.Start()

	// Assign new watcher to the connection
	d.Watcher = newWatcher
	return nil
}

func (d *ConnectionData) fileChangesUpdateSchema(events []fsnotify.Event) bool {
	for _, e := range events {
		if e.Op == fsnotify.Chmod {
			continue
		}
		for _, p := range d.SchemaChangingWatchPaths {
			match := filehelpers.Match(p, e.Name)
			if match {
				return true
			}
		}
	}
	return false
}
