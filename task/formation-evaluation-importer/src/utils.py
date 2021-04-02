from typing import Iterator


def chunker(seq: list, size: int) -> Iterator[list]:
    for idx in range(0, len(seq), size):
        yield seq[idx : idx + size]
