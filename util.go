package proxy

import "math"

/**
 * number => byte (ascii decimal)
 * -123 => 45 49
 * 123 => 49, 50, 51
 */
func NumberToBytes(num int) []byte {
	if num < 0 {
		return append([]byte("-"), NumberToBytes(num*-1)...)
	}
	numBytes := make([]byte, 0)
	for {
		if num < 10 {
			numBytes = append([]byte{byte(48 + num)}, numBytes...)
			break
		}
		b := num % 10
		numBytes = append([]byte{byte(48 + b)}, numBytes...)
		num = num / 10
	}

	return numBytes
}

func BytesToNumber(numBytes []byte) int {
	if numBytes[0] == '-' {
		return -1 * BytesToNumber(numBytes[1:])
	}
	var number int
	carry := len(numBytes) - 1
	for pos, b := range numBytes {
		number += int(b-48) * int(math.Pow10(carry-pos))
	}

	return number
}
