package version

// ProtocolVersion marks the protobuf interface version
//// Update this if breaking changes are made to the protobuf spec (for example column types)
var ProtocolVersion int64 = 20210701

// Version is the semantic version number reflecting the current release.
var Version = "1.3.0"
