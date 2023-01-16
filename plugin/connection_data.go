package plugin

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"log"
	"path"

	"github.com/fsnotify/fsnotify"
	"github.com/turbot/go-kit/filewatcher"
	"github.com/turbot/steampipe-plugin-sdk/v5/getter"
)

// ConnectionData is the data stored by the plugin which is connection dependent.
type ConnectionData struct {
	// map of all the tables in the plugin, keyed by the table name
	TableMap   map[string]*Table
	Connection *Connection
	// schema may be connection specific for dynamic schemas
	Schema *grpc.PluginSchema
	// this is connection specific filewatcher to watch for the changes in files (if needed)
	Watcher *filewatcher.FileWatcher
	Plugin  *Plugin
}

// GetConnectionTempDir appends the connection name to the plugin temporary directory path
func (d *ConnectionData) GetConnectionTempDir(pluginTempDir string) string {
	return path.Join(pluginTempDir, d.Connection.Name)
}

func (d *ConnectionData) updateWatchPaths(watchPaths []string, p *Plugin) error {
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
	for _, watchPath := range watchPaths {
		dest, globPattern, err := getter.GetFiles(watchPath, connTempDir)
		if err != nil {
			log.Printf("[WARN] ConnectionData updateWatchPaths - error resolving source path %s: %s", watchPath, err.Error())
			continue
		}
		opts.Directories = append(opts.Directories, dest)
		opts.Include = append(opts.Include, globPattern)
	}

	// if we have no paths, do not start a watcher
	if len(opts.Directories) == 0 {
		log.Printf("[TRACE] ConnectionData updateWatchPaths - no watch paths resolved - not creating watcher")
		return nil
	}
	// Add the callback function for the filewatchers to watcher options
	opts.OnChange = func(events []fsnotify.Event) {
		log.Printf("[TRACE] watched connection files changed")
		p.WatchedFileChangedFunc(context.Background(), p, d.Connection, events)
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
