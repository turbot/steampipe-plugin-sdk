package plugin

import (
	"context"
	"log"
	"path"

	"github.com/fsnotify/fsnotify"
	"github.com/turbot/go-kit/filewatcher"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// ConnectionData is the data stored by the plugin which is connection dependent.
type ConnectionData struct {
	// TableMap is a map of all the tables in the plugin, keyed by the table name
	TableMap map[string]*Table
	// connection this plugin is instantiated for
	Connection *Connection
	// schema - this may be connection specific for dynamic schemas
	Schema map[string]*proto.TableSchema
	// FileWatcher - this is connection specific to watch for the changes in files
	Watcher *filewatcher.FileWatcher
	// WatchPaths - list of file paths to be watched
	WatchPaths []string
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

	// set watch paths
	d.WatchPaths = watchPaths

	// create WatcherOptions
	connTempDir := d.GetConnectionTempDir(p.tempDir)
	opts := filewatcher.WatcherOptions{}

	// Iterate through watch paths to resolve and
	// add resolved paths to file watcher options
	log.Printf("[TRACE] ConnectionData.updateWatchPaths - create watcher options from the watchPaths %v", watchPaths)
	for _, path := range watchPaths {
		dest, globPattern, err := ResolveSourcePath(path, connTempDir)
		if err != nil {
			log.Printf("[WARN] ConnectionData updateWatchPaths - error resolving source path %s: %s", path, err.Error())
			continue
		}
		opts.Directories = append(opts.Directories, dest)
		opts.Include = append(opts.Include, globPattern)
	}

	// if we have no paths, do not start a watcher
	if len(opts.Directories) == 0 {
		log.Printf("[WARN] ConnectionData updateWatchPaths - no watch paths resolved - not creating watcher")
		return nil
	}
	// Add the callback function for the filewatchers to watcher options
	opts.OnChange = func(events []fsnotify.Event) {
		// Log for testing
		log.Printf("[WARN] ConnectionData updateWatchPaths - callback function called")
		p.WatchedFileChangedFunc(context.Background(), p, d.Connection, events)
	}

	// Get the new file watcher from file options
	newWatcher, err := filewatcher.NewWatcher(&opts)
	log.Printf("[WARN] ConnectionData updateWatchPaths - create the new file watcher")
	if err != nil {
		log.Printf("[WARN] ConnectionData.updateWatchPaths -failed to create a new file watcher: %s", err.Error())
		return err
	}
	log.Printf("[TRACE] ConnectionData.updateWatchPaths - created the new file watcher")

	// Start new watcher
	log.Printf("[WARN] ConnectionData updateWatchPaths - start the new file watcher")
	newWatcher.Start()

	// Assign new watcher to the connection
	d.Watcher = newWatcher
	log.Printf("[WARN] ConnectionData updateWatchPaths - attach the new file watcher to connection data")
	return nil
}
