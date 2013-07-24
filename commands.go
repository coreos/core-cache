package main

const (
	OpGet = uint8(iota)
	OpSet
	OpAdd
	OpReplace
	OpDelete
	OpIncrement
	OpDecrement
	OpQuit
	OpFlush
	OpGetQ
	OpNoop
	OpVersion
	OpGetK
	OpGetKQ
	OpAppend
	OpPrepend
	OpStat
	OpSetQ
	OpAddQ
	OpReplaceQ
	OpDeleteQ
	OpIncrementQ
	OpDecrementQ
	OpQuitQ
	OpFlushQ
	OpAppendQ
	OpPrependQ
)

var textCommands = map[string]bool{
	// Storage commands
	"set":     true,
	"add":     true,
	"replace": true,
	"append":  true,
	"prepend": true,

	// Retrieval command
	"get":  true,
	"gets": true,

	// Deletion
	"delete": true,

	// Increment/Decrement
	"incr": true,
	"decr": true,

	// Touch
	"touch": true,
}

var binaryCommands = map[uint8]bool{
	OpGet:       true,
	OpSet:       true,
	OpAdd:       true,
	OpReplace:   true,
	OpDelete:    true,
	OpIncrement: true,
	OpDecrement: true,
	//OpQuit
	//OpFlush
	OpGetQ: true,
	//OpNoop
	//OpVersion
	OpGetK:    true,
	OpGetKQ:   true,
	OpAppend:  true,
	OpPrepend: true,
	//OpStat
	OpSetQ:       true,
	OpAddQ:       true,
	OpReplaceQ:   true,
	OpDeleteQ:    true,
	OpIncrementQ: true,
	OpDecrementQ: true,
	OpQuitQ:      true,
	//OpFlushQ
	OpAppendQ:  true,
	OpPrependQ: true,
}
