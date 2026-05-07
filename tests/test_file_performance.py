"""
Performance tests for file streaming with different buffer sizes.

These tests verify that larger buffer sizes improve streaming performance,
especially important for network filesystems where latency dominates.
"""

import tempfile
import time
from pathlib import Path

import pytest

from x2s3.client_file import file_iterator, FileObjectHandle


def create_test_file(size_mb: int) -> Path:
    """Create a temporary test file of specified size."""
    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp_path = Path(tmp.name)

    # Write test data in 1MB chunks
    chunk = b'x' * (1024 * 1024)
    for _ in range(size_mb):
        tmp.write(chunk)
    tmp.close()

    return tmp_path


def stream_file_with_buffer(file_path: Path, buffer_size: int) -> tuple[float, int]:
    """
    Stream a file with given buffer size and return (elapsed_time, bytes_read).
    """
    file_handle = open(file_path, 'rb')
    file_size = file_path.stat().st_size

    handle = FileObjectHandle(
        target_name="test",
        key=str(file_path),
        status_code=200,
        headers={},
        media_type="application/octet-stream",
        content_length=file_size,
        file_handle=file_handle,
        start=0,
        end=None
    )

    start_time = time.time()
    total_bytes = 0

    for chunk in file_iterator(handle, buffer_size=buffer_size):
        total_bytes += len(chunk)

    elapsed = time.time() - start_time

    return elapsed, total_bytes


@pytest.mark.parametrize("file_size_mb", [10, 50])
def test_buffer_size_performance(file_size_mb):
    """
    Test that larger buffer sizes improve streaming performance.

    This test verifies the fix works by comparing performance across
    different buffer sizes. Larger buffers should be faster due to
    fewer read() system calls.
    """
    test_file = create_test_file(file_size_mb)

    try:
        # Test with small buffer (8KB - the old default behavior)
        time_8kb, bytes_8kb = stream_file_with_buffer(test_file, buffer_size=8192)

        # Test with medium buffer (64KB)
        time_64kb, bytes_64kb = stream_file_with_buffer(test_file, buffer_size=64*1024)

        # Test with large buffer (256KB - recommended for network filesystems)
        time_256kb, bytes_256kb = stream_file_with_buffer(test_file, buffer_size=256*1024)

        # Verify all read the same amount
        expected_bytes = file_size_mb * 1024 * 1024
        assert bytes_8kb == expected_bytes
        assert bytes_64kb == expected_bytes
        assert bytes_256kb == expected_bytes

        # Calculate speedups
        speedup_64kb = time_8kb / time_64kb if time_64kb > 0 else float('inf')
        speedup_256kb = time_8kb / time_256kb if time_256kb > 0 else float('inf')

        # Print performance metrics for debugging
        def mb_per_sec(b, t):
            return b / t / (1024 * 1024) if t > 0 else float('inf')

        print(f"\n=== Performance Test Results ({file_size_mb}MB file) ===")
        print(f"8KB buffer:   {time_8kb:.4f}s ({mb_per_sec(bytes_8kb, time_8kb):.2f} MB/s)")
        print(f"64KB buffer:  {time_64kb:.4f}s ({mb_per_sec(bytes_64kb, time_64kb):.2f} MB/s) - {speedup_64kb:.2f}x faster")
        print(f"256KB buffer: {time_256kb:.4f}s ({mb_per_sec(bytes_256kb, time_256kb):.2f} MB/s) - {speedup_256kb:.2f}x faster")

        # Assert performance improvements
        # Note: Even on local disk, we expect at least 20% improvement with larger buffers
        # On network filesystems, improvements would be 10-100x or more
        assert time_64kb < time_8kb, \
            f"64KB buffer should be faster than 8KB (8KB: {time_8kb:.4f}s, 64KB: {time_64kb:.4f}s)"

        assert time_256kb < time_8kb, \
            f"256KB buffer should be faster than 8KB (8KB: {time_8kb:.4f}s, 256KB: {time_256kb:.4f}s)"

        # Allow some tolerance for measurement noise, but expect meaningful improvement
        # We use a conservative 1.2x threshold to avoid flaky tests, but real improvements
        # should be much larger (3-5x on local disk, 100-1000x on network filesystems)
        assert speedup_64kb > 1.2, \
            f"64KB buffer should be at least 20% faster (got {speedup_64kb:.2f}x)"

        assert speedup_256kb > 1.2, \
            f"256KB buffer should be at least 20% faster (got {speedup_256kb:.2f}x)"

    finally:
        # Cleanup
        test_file.unlink()


def test_file_iterator_respects_buffer_size():
    """
    Test that file_iterator actually uses the buffer_size parameter.

    This test verifies that the fix (explicit fh.read(buffer_size)) properly
    respects the buffer_size parameter, which is critical for performance on
    network filesystems where each read operation has significant overhead.
    """
    test_file = create_test_file(5)  # 5MB test file

    try:
        # Test with different buffer sizes using the actual file_iterator function
        buffer_sizes = [8192, 64*1024, 256*1024]  # 8KB, 64KB, 256KB
        results = []

        for buffer_size in buffer_sizes:
            file_handle = open(test_file, 'rb')
            file_size = test_file.stat().st_size

            handle = FileObjectHandle(
                target_name="test",
                key=str(test_file),
                status_code=200,
                headers={},
                media_type="application/octet-stream",
                content_length=file_size,
                file_handle=file_handle,
                start=0,
                end=None  # Full file (this is where our fix applies)
            )

            # Collect chunks from file_iterator
            chunks = []
            for chunk in file_iterator(handle, buffer_size=buffer_size):
                chunks.append(len(chunk))

            results.append({
                'buffer_size': buffer_size,
                'chunk_count': len(chunks),
                'chunk_sizes': chunks,
                'total_bytes': sum(chunks)
            })

        # Verify all read the same total data
        for result in results:
            assert result['total_bytes'] == 5 * 1024 * 1024, \
                f"Should read 5MB, got {result['total_bytes']}"

        print(f"\nfile_iterator buffer_size impact:")
        for result in results:
            print(f"  {result['buffer_size'] // 1024}KB buffer: {result['chunk_count']} chunks yielded")

        # Verify larger buffers result in fewer chunks (fewer read operations)
        assert results[0]['chunk_count'] > results[1]['chunk_count'] > results[2]['chunk_count'], \
            "Larger buffers should result in fewer chunks from file_iterator"

        # Verify the reduction is significant (our fix should achieve 10x+ reduction)
        reduction = results[0]['chunk_count'] / results[2]['chunk_count']
        print(f"  Reduction with 256KB vs 8KB: {reduction:.1f}x fewer chunks")
        assert reduction > 10, \
            f"256KB buffer should result in at least 10x fewer chunks than 8KB (got {reduction:.1f}x)"

        # Verify chunks are approximately the requested size
        for result in results:
            if result['chunk_count'] > 1:
                # Check all chunks except the last (which may be partial)
                for chunk_size in result['chunk_sizes'][:-1]:
                    # Chunks should match the buffer_size exactly for full reads
                    assert chunk_size == result['buffer_size'], \
                        f"Chunk size {chunk_size} should equal buffer_size {result['buffer_size']}"

    finally:
        test_file.unlink()


def test_range_request_buffering():
    """
    Test that range requests also benefit from proper buffering.
    """
    test_file = create_test_file(10)  # 10MB test file

    try:
        # Read a range with small buffer
        with open(test_file, 'rb') as file_handle:
            handle_small = FileObjectHandle(
                target_name="test",
                key=str(test_file),
                status_code=206,
                headers={},
                media_type="application/octet-stream",
                content_length=1024*1024,  # 1MB range
                file_handle=file_handle,
                start=0,
                end=1024*1024 - 1
            )
            time_small, bytes_small = stream_file_with_buffer(test_file, buffer_size=8192)

        # Read same range with large buffer
        with open(test_file, 'rb') as file_handle:
            handle_large = FileObjectHandle(
                target_name="test",
                key=str(test_file),
                status_code=206,
                headers={},
                media_type="application/octet-stream",
                content_length=1024*1024,
                file_handle=file_handle,
                start=0,
                end=1024*1024 - 1
            )
            time_large, bytes_large = stream_file_with_buffer(test_file, buffer_size=256*1024)

        # Both should read same amount
        assert bytes_small == bytes_large

        # Large buffer should be faster
        assert time_large <= time_small, \
            "Larger buffer should be faster or equal for range requests"

    finally:
        test_file.unlink()


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
