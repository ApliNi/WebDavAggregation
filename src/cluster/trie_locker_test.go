package cluster

import (
	"sync"
	"testing"
	"time"
)

func TestPathLocker_PrefixTrie(t *testing.T) {
	pl := NewPathLocker()

	t.Run("single path exclusive", func(t *testing.T) {
		u1, ok1 := pl.TryLock("/a/b/c.txt")
		if !ok1 {
			t.Fatal("expected ok1")
		}
		_, ok2 := pl.TryLock("/a/b/c.txt")
		if ok2 {
			t.Fatal("expected ok2 to fail")
		}
		u1()
		u3, ok3 := pl.TryLock("/a/b/c.txt")
		if !ok3 {
			t.Fatal("expected ok3 after unlock")
		}
		u3()
	})

	t.Run("subtree blocks child", func(t *testing.T) {
		uSub, okSub := pl.TryLockSubtree("/photos")
		if !okSub {
			t.Fatal("expected okSub")
		}
		// child single lock should fail
		_, okChild := pl.TryLock("/photos/2026/trip.jpg")
		if okChild {
			t.Fatal("expected child lock to fail when subtree locked")
		}
		// child subtree should fail
		_, okSub2 := pl.TryLockSubtree("/photos/2026")
		if okSub2 {
			t.Fatal("expected nested subtree lock to fail")
		}
		// parent subtree should fail
		_, okParentSub := pl.TryLockSubtree("/")
		if okParentSub {
			t.Fatal("expected parent subtree lock to fail")
		}
		// sibling should succeed
		uSib, okSib := pl.TryLock("/docs/readme.md")
		if !okSib {
			t.Fatal("expected sibling lock to succeed")
		}
		uSib()
		uSub()

		// after unlock, child should succeed
		uChild, okChild2 := pl.TryLock("/photos/2026/trip.jpg")
		if !okChild2 {
			t.Fatal("expected child to succeed after subtree unlock")
		}
		uChild()
	})

	t.Run("child blocks ancestor subtree", func(t *testing.T) {
		uChild, okChild := pl.TryLock("/photos/2026/trip.jpg")
		if !okChild {
			t.Fatal("expected okChild")
		}
		_, okSub := pl.TryLockSubtree("/photos")
		if okSub {
			t.Fatal("expected ancestor subtree to fail when child locked")
		}
		uChild()

		uSub2, okSub2 := pl.TryLockSubtree("/photos")
		if !okSub2 {
			t.Fatal("expected ancestor subtree to succeed after child unlock")
		}
		uSub2()
	})

	t.Run("concurrency test", func(t *testing.T) {
		const goroutines = 20
		var wg sync.WaitGroup
		counter := 0
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				unlock := pl.Lock("/concurrent/test.txt")
				counter++
				time.Sleep(2 * time.Millisecond)
				unlock()
			}()
		}
		wg.Wait()
		if counter != goroutines {
			t.Fatalf("expected counter %d, got %d", goroutines, counter)
		}
	})
}
