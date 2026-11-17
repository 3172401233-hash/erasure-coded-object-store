"""Arithmetic in GF(2^8) with the irreducible polynomial x^8+x^4+x^3+x+1 (0x11D)."""

POLY = 0x11D
_ORDER = 255

_EXP = [0] * (2 * _ORDER)
_LOG = [0] * 256

_x = 1
for _i in range(_ORDER):
    _EXP[_i] = _x
    _LOG[_x] = _i
    _x <<= 1
    if _x & 0x100:
        _x ^= POLY
for _i in range(_ORDER, 2 * _ORDER):
    _EXP[_i] = _EXP[_i - _ORDER]


def add(a, b):
    return a ^ b


def sub(a, b):
    return a ^ b


def mul(a, b):
    if a == 0 or b == 0:
        return 0
    return _EXP[_LOG[a] + _LOG[b]]


def div(a, b):
    if b == 0:
        raise ZeroDivisionError("division by zero in GF(256)")
    if a == 0:
        return 0
    return _EXP[_LOG[a] - _LOG[b] + _ORDER]


def inv(a):
    if a == 0:
        raise ZeroDivisionError("inverse of zero in GF(256)")
    return _EXP[_ORDER - _LOG[a]]


def pow_gf(a, n):
    if n == 0:
        return 1
    if a == 0:
        return 0
    return _EXP[(_LOG[a] * n) % _ORDER]


def mat_mul(left, right):
    """Multiply two matrices over GF(256)."""
    rows, inner, cols = len(left), len(right), len(right[0])
    if any(len(r) != inner for r in left):
        raise ValueError("ragged left matrix")
    if any(len(r) != cols for r in right):
        raise ValueError("ragged right matrix")
    out = []
    for r in range(rows):
        row = []
        for c in range(cols):
            acc = 0
            for t in range(inner):
                acc ^= mul(left[r][t], right[t][c])
            row.append(acc)
        out.append(row)
    return out


def mat_inverse(matrix):
    """Gauss-Jordan inversion of a square matrix over GF(256)."""
    n = len(matrix)
    if any(len(r) != n for r in matrix):
        raise ValueError("matrix must be square")
    aug = [list(matrix[i]) + [1 if i == j else 0 for j in range(n)] for i in range(n)]
    for col in range(n):
        pivot = None
        for r in range(col, n):
            if aug[r][col] != 0:
                pivot = r
                break
        if pivot is None:
            raise ValueError("matrix is singular")
        aug[col], aug[pivot] = aug[pivot], aug[col]
        scale = inv(aug[col][col])
        aug[col] = [mul(v, scale) for v in aug[col]]
        for r in range(n):
            if r != col and aug[r][col] != 0:
                factor = aug[r][col]
                aug[r] = [a ^ mul(factor, b) for a, b in zip(aug[r], aug[col])]
    return [row[n:] for row in aug]
