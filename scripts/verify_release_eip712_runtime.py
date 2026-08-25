#!/usr/bin/env python3
"""Verify the reviewed EIP-712 pysha3 runtime substitution in an image."""

import sha3
from eip712_structs import EIP712Struct, String, make_domain


class HummingbotRuntimeProbe(EIP712Struct):
    """Small deterministic payload that exercises EIP-712 encoding."""

    message = String()


payload = HummingbotRuntimeProbe(message="release-lock-runtime-smoke")
domain = make_domain(
    name="Hummingbot",
    version="1",
    chainId=1,
    verifyingContract="0x0000000000000000000000000000000000000001",  # noqa: mock
)
signable = payload.signable_bytes(domain)

assert len(signable) == 66, f"unexpected EIP-712 signable byte length: {len(signable)}"
assert signable[:2] == b"\x19\x01", f"unexpected EIP-712 prefix: {signable[:2].hex()}"
assert (
    sha3.keccak_256(b"").hexdigest()
    == "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"  # noqa: mock
), "safe-pysha3 does not provide the expected Keccak-256 implementation"

print("release EIP-712 runtime smoke passed")
