package xitdb

type WriteableData interface {
	writeableData()
}

type Uint struct {
	Value uint64
}

func (Uint) writeableData() {}

func NewUint(value uint64) Uint {
	return Uint{Value: value}
}

type Int struct {
	Value int64
}

func (Int) writeableData() {}

func NewInt(value int64) Int {
	return Int{Value: value}
}

type Float struct {
	Value float64
}

func (Float) writeableData() {}

func NewFloat(value float64) Float {
	return Float{Value: value}
}

type Bytes struct {
	Value     []byte
	FormatTag []byte
}

func (Bytes) writeableData() {}

func NewBytes(value []byte) Bytes {
	return Bytes{Value: value}
}

func NewTaggedBytes(value []byte, formatTag []byte) Bytes {
	return Bytes{Value: value, FormatTag: formatTag}
}

func NewString(value string) Bytes {
	return Bytes{Value: []byte(value)}
}

func NewTaggedString(value string, formatTag string) Bytes {
	return Bytes{Value: []byte(value), FormatTag: []byte(formatTag)}
}

func (b Bytes) IsShort() bool {
	totalSize := 8
	if b.FormatTag != nil {
		totalSize = 6
	}
	if len(b.Value) > totalSize {
		return false
	}
	for _, v := range b.Value {
		if v == 0 {
			return false
		}
	}
	return true
}

// Slot also implements WriteableData
func (Slot) writeableData() {}
