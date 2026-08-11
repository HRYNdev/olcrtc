package tunnelcore

import (
	"context"
	"errors"
	"io"
)

// CopyCounts reports bytes copied in both directions.
type CopyCounts struct {
	LeftToRight uint64
	RightToLeft uint64
}

type copyResult struct {
	leftToRight bool
	bytes       uint64
	err         error
}

// CopyBidirectional copies until both directions finish or ctx is canceled.
// A clean EOF half-closes the destination when supported; errors close both sides.
func CopyBidirectional(
	ctx context.Context,
	left io.ReadWriteCloser,
	right io.ReadWriteCloser,
) (CopyCounts, error) {
	results := make(chan copyResult, 2)
	go copyOneWay(results, true, right, left)
	go copyOneWay(results, false, left, right)

	var counts CopyCounts
	var errs []error
	for completed := 0; completed < 2; completed++ {
		select {
		case result := <-results:
			setCopyCount(&counts, result)
			if result.err != nil {
				errs = append(errs, result.err)
				_ = left.Close()
				_ = right.Close()
				continue
			}
			closeCopyWrite(result.leftToRight, left, right)
		case <-ctx.Done():
			_ = left.Close()
			_ = right.Close()
			for ; completed < 2; completed++ {
				result := <-results
				setCopyCount(&counts, result)
				if result.err != nil {
					errs = append(errs, result.err)
				}
			}
			return counts, errors.Join(append(errs, ctx.Err())...)
		}
	}
	return counts, errors.Join(errs...)
}

func copyOneWay(results chan<- copyResult, leftToRight bool, dst io.Writer, src io.Reader) {
	n, err := io.Copy(dst, src)
	result := copyResult{leftToRight: leftToRight, err: err}
	if n > 0 {
		result.bytes = uint64(n)
	}
	results <- result
}

func setCopyCount(counts *CopyCounts, result copyResult) {
	if result.leftToRight {
		counts.LeftToRight = result.bytes
		return
	}
	counts.RightToLeft = result.bytes
}

func closeCopyWrite(leftToRight bool, left, right io.Closer) {
	dst := left
	if leftToRight {
		dst = right
	}
	if closer, ok := dst.(interface{ CloseWrite() error }); ok {
		_ = closer.CloseWrite()
		return
	}
	_ = dst.Close()
}
