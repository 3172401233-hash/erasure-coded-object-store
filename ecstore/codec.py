"""Systematic Reed-Solomon codec over GF(256).

The (k + m) x k encoding matrix is derived from a Vandermonde matrix that is
normalised so its top k rows form the identity: data shards are therefore
stored verbatim and any k of the k + m shards reconstruct the object.
"""

from . import gf256


def build_matrix(k, m):
    """Return the systematic (k + m) x k encoding matrix."""
    if k < 1 or m < 0:
        raise ValueError("k must be >= 1 and m must be >= 0")
    if k + m > 256:
        raise ValueError("k + m must not exceed 256")
    vandermonde = [
        [gf256.pow_gf(i, j) for j in range(k)] for i in range(k + m)
    ]
    top_inverse = gf256.mat_inverse(vandermonde[:k])
    return gf256.mat_mul(vandermonde, top_inverse)


def split_object(data, k):
    """Split *data* into k equal-length shards, zero-padding the tail."""
    shard_length = (len(data) + k - 1) // k
    shards = []
    for i in range(k):
        chunk = data[i * shard_length:(i + 1) * shard_length]
        shards.append(bytes(chunk) + b"\x00" * (shard_length - len(chunk)))
    return shards, shard_length


def apply_matrix(rows, data_shards):
    """Multiply matrix rows by the data-shard columns, byte by byte."""
    shard_length = len(data_shards[0])
    out = []
    for row in rows:
        buf = bytearray(shard_length)
        for coef, shard in zip(row, data_shards):
            if coef == 0:
                continue
            if coef == 1:
                for pos, byte in enumerate(shard):
                    buf[pos] ^= byte
            else:
                for pos, byte in enumerate(shard):
                    buf[pos] ^= gf256.mul(coef, byte)
        out.append(bytes(buf))
    return out


def encode(data, k, m):
    """Encode *data* into k + m shards; returns (shards, shard_length)."""
    matrix = build_matrix(k, m)
    data_shards, shard_length = split_object(data, k)
    parity_shards = apply_matrix(matrix[k:], data_shards)
    return data_shards + parity_shards, shard_length


def decode(available, k, m, shard_length):
    """Rebuild the k data shards from {shard_index: payload}.

    Raises ValueError if fewer than k shards are supplied.
    """
    if len(available) < k:
        raise ValueError(
            "need %d shards to decode, only %d available" % (k, len(available))
        )
    matrix = build_matrix(k, m)
    chosen = sorted(available)[:k]
    sub_matrix = [matrix[i] for i in chosen]
    decode_matrix = gf256.mat_inverse(sub_matrix)
    selected = [available[i] for i in chosen]
    return apply_matrix(decode_matrix, selected)


def reencode_shard(index, data_shards, k, m):
    """Recompute shard *index* from the full set of data shards."""
    matrix = build_matrix(k, m)
    return apply_matrix([matrix[index]], data_shards)[0]
