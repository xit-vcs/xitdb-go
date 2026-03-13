package xitdb

type WriteableData interface {
	writeableData()
}

type UintData struct {
	Value uint64
}

func (UintData) writeableData() {}

type IntData struct {
	Value int64
}

func (IntData) writeableData() {}

type FloatData struct {
	Value float64
}

func (FloatData) writeableData() {}

type BytesData struct {
	Value     []byte
	FormatTag []byte
}

func (BytesData) writeableData() {}

func NewBytesData(value []byte) BytesData {
	return BytesData{Value: value}
}

func NewBytesDataWithFormat(value []byte, formatTag []byte) BytesData {
	return BytesData{Value: value, FormatTag: formatTag}
}

func NewBytesDataFromString(value string) BytesData {
	return BytesData{Value: []byte(value)}
}

func NewBytesDataFromStringWithFormat(value string, formatTag string) BytesData {
	return BytesData{Value: []byte(value), FormatTag: []byte(formatTag)}
}

func (b BytesData) IsShort() bool {
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
