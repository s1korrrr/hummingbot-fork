#!/usr/bin/env python3
"""Verify the reviewed EIP-712 pysha3 runtime substitution in an image."""

import sha3
from eip712_structs import EIP712Struct, String, make_domain


class HummingbotRuntimeProbe(EIP712Struct):
    """Small deterministic payload that exercises EIP-712 encoding."""

    message = String()


EXPECTED_SIGNABLE_HEX = (
    "1901de57171073f97873bc93388ae5e87afd47d803e4b4a59a7a402a0eb71b7aa2e6"  # noqa: mock
    "64e15bdc268f69b1f35c21c9559e329a2f3587fe22a9800badf545aec280d842"  # noqa: mock
)
EXPECTED_SIGNABLE_KECCAK = "358eb3432f7a6fdfb6bbec6fcc85bf845d3feea114fb95722bbbf854a934453e"  # noqa: mock
EXPECTED_EMPTY_KECCAK = "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"  # noqa: mock


def verify_signable(signable: bytes) -> None:
    signable_hex = signable.hex()
    if signable_hex != EXPECTED_SIGNABLE_HEX:
        raise RuntimeError(f"unexpected EIP-712 signable bytes: {signable_hex}")

    signable_keccak = sha3.keccak_256(signable).hexdigest()
    if signable_keccak != EXPECTED_SIGNABLE_KECCAK:
        raise RuntimeError(f"unexpected EIP-712 signable Keccak-256: {signable_keccak}")


def verify_runtime() -> None:
    payload = HummingbotRuntimeProbe(message="release-lock-runtime-smoke")
    domain = make_domain(
        name="Hummingbot",
        version="1",
        chainId=1,
        verifyingContract="0x0000000000000000000000000000000000000001",  # noqa: mock
    )
    verify_signable(payload.signable_bytes(domain))

    empty_keccak = sha3.keccak_256(b"").hexdigest()
    if empty_keccak != EXPECTED_EMPTY_KECCAK:
        raise RuntimeError(f"unexpected empty-input Keccak-256: {empty_keccak}")


if __name__ == "__main__":
    verify_runtime()
    print("release EIP-712 runtime smoke passed")
