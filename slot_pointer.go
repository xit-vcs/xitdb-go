package xitdb

type SlotPointer struct {
	Position *int64
	Slot     Slot
}

func (sp SlotPointer) WithSlot(slot Slot) SlotPointer {
	return SlotPointer{Position: sp.Position, Slot: slot}
}
