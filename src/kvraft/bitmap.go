package kvraft

import (
	"strings"
)

const (
	bitSize = 8
)

var bitmask = []byte{1, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6, 1 << 7}

type bitmap struct {
	Bits     []byte
	BitCount uint64 // 已填入数字的数量
}

func NewBitmap() *bitmap {
	return &bitmap{
		Bits:     make([]byte, 1, 100),
		BitCount: 0,
	}
}

// 填入数字
func (bm *bitmap) Set(num uint64) {
	byteIndex, bitPos := bm.offset(num)
	// 1 左移 bitPos 位 进行 按位或 (置为 1)
	bm.Bits[byteIndex] |= bitmask[bitPos]
	bm.BitCount++
}

// 清除填入的数字
func (bm *bitmap) Reset(num uint64) {
	byteIndex, bitPos := bm.offset(num)
	// 重置为空位 (重置为 0)
	bm.Bits[byteIndex] &= ^bitmask[bitPos]
	bm.BitCount--
}

// 数字是否在位图中
func (bm *bitmap) Contain(num uint64) bool {
	byteIndex := num / bitSize
	if byteIndex >= uint64(len(bm.Bits)) {
		return false
	}
	bitPos := num % bitSize
	// 右移 bitPos 位 和 1 进行 按位与
	return !(bm.Bits[byteIndex]&bitmask[bitPos] == 0)
}

func (bm *bitmap) offset(num uint64) (byteIndex uint64, bitPos byte) {
	byteIndex = num / bitSize // 字节索引
	byteNum := (num + bitSize) / bitSize
	if n := byteNum - uint64(len(bm.Bits)); n > 0 {
		var i uint64
		for ; i < n; i++ {
			bm.Bits = append(bm.Bits, 0)
		}
		return
	}
	bitPos = byte(num % bitSize) // bit位置
	return byteIndex, bitPos
}

// 位图的容量
func (bm *bitmap) Size() uint64 {
	return uint64(len(bm.Bits) * bitSize)
}

// 是否空位图
func (bm *bitmap) IsEmpty() bool {
	return bm.BitCount == 0
}

// 已填入的数字个数
func (bm *bitmap) Count() uint64 {
	return bm.BitCount
}

// 获取填入的数字切片
func (bm *bitmap) GetData() []uint64 {
	var data []uint64
	count := bm.Size()
	for index := uint64(0); index < count; index++ {
		if bm.Contain(index) {
			data = append(data, index)
		}
	}
	return data
}

func (bm *bitmap) String() string {
	var sb strings.Builder
	for index := len(bm.Bits) - 1; index >= 0; index-- {
		sb.WriteString(byteToBinaryString(bm.Bits[index]))
		sb.WriteString(" ")
	}
	return sb.String()
}

func byteToBinaryString(data byte) string {
	var sb strings.Builder
	for index := 0; index < bitSize; index++ {
		if (bitmask[7-index] & data) == 0 {
			sb.WriteString("0")
		} else {
			sb.WriteString("1")
		}
	}
	return sb.String()
}
