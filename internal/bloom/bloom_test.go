package bloom

import (
	"testing"
)

func TestBloomBasic(t *testing.T) {
	f := New(100, 0.01)
	keys := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	for _, k := range keys {
		f.Add([]byte(k))
	}
	for _, k := range keys {
		if !f.MayContain([]byte(k)) {
			t.Errorf("MayContain(%q) = false, want true", k)
		}
	}
}

func TestBloomMarshalRoundTrip(t *testing.T) {
	f := New(1000, 0.01)
	for i := 0; i < 500; i++ {
		f.Add([]byte("key" + itoa(i)))
	}
	data := f.MarshalBinary()

	g, err := UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for i := 0; i < 500; i++ {
		if !g.MayContain([]byte("key" + itoa(i))) {
			t.Errorf("after roundtrip MayContain(key%d) = false", i)
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	// With 1000 keys at 1% target, the false-positive rate over many absent
	// probes should be reasonably small.
	f := New(1000, 0.01)
	for i := 0; i < 1000; i++ {
		f.Add([]byte("present" + itoa(i)))
	}
	fp := 0
	trials := 10000
	for i := 0; i < trials; i++ {
		if f.MayContain([]byte("absent" + itoa(i))) {
			fp++
		}
	}
	rate := float64(fp) / float64(trials)
	if rate > 0.05 {
		t.Errorf("false-positive rate = %.3f, want <= 0.05", rate)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
