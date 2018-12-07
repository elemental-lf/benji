# We include this version of aes_keywrap to prevent a Git dependency in
# setup.py. The more recent Python 3 compatible version from GitHub is
# not present on PyPi unfortunately.
#
# Source: https://github.com/kurtbrose/aes_keywrap
# MIT License
# Copyright (c) 2017 Kurt Rose
'''
Key wrapping and unwrapping as defined in RFC 3394.  
Also a padding mechanism that was used in openssl at one time.  
The purpose of this algorithm is to encrypt a key multiple times to add an extra layer of security.
'''
import struct
# TODO: dependency flexibility; make pip install aes_keywrap[cryptography], etc work
from Crypto.Cipher import AES

QUAD = struct.Struct('>Q')

def aes_unwrap_key_and_iv(kek, wrapped):
    n = len(wrapped)//8 - 1
    #NOTE: R[0] is never accessed, left in for consistency with RFC indices
    R = [None]+[wrapped[i*8:i*8+8] for i in range(1, n+1)]
    A = QUAD.unpack(wrapped[:8])[0]
    decrypt = AES.new(kek, AES.MODE_ECB).decrypt
    for j in range(5,-1,-1): #counting down
        for i in range(n, 0, -1): #(n, n-1, ..., 1)
            ciphertext = QUAD.pack(A^(n*j+i)) + R[i]
            B = decrypt(ciphertext)
            A = QUAD.unpack(B[:8])[0]
            R[i] = B[8:]
    return b"".join(R[1:]), A


def aes_unwrap_key(kek, wrapped, iv=0xa6a6a6a6a6a6a6a6):
    '''
    key wrapping as defined in RFC 3394
    http://www.ietf.org/rfc/rfc3394.txt
    '''
    key, key_iv = aes_unwrap_key_and_iv(kek, wrapped)
    if key_iv != iv:
        raise ValueError("Integrity Check Failed: "+hex(key_iv)+" (expected "+hex(iv)+")")
    return key


def aes_unwrap_key_withpad(kek, wrapped):
    '''
    alternate initial value for aes key wrapping, as defined in RFC 5649 section 3
    http://www.ietf.org/rfc/rfc5649.txt
    '''
    if len(wrapped) == 16:
        plaintext = AES.new(kek, AES.MODE_ECB).decrypt(wrapped)
        key, key_iv = plaintext[:8], plaintext[8:]
    else:
        key, key_iv = aes_unwrap_key_and_iv(kek, wrapped)
    key_iv = "{0:016X}".format(key_iv)
    if key_iv[:8] != "A65959A6":
        raise ValueError("Integrity Check Failed: "+key_iv[:8]+" (expected A65959A6)")
    key_len = int(key_iv[8:], 16)
    return key[:key_len]

def aes_wrap_key(kek, plaintext, iv=0xa6a6a6a6a6a6a6a6):
    n = len(plaintext)//8
    R = [None]+[plaintext[i*8:i*8+8] for i in range(0, n)]
    A = iv
    encrypt = AES.new(kek, AES.MODE_ECB).encrypt
    for j in range(6):
        for i in range(1, n+1):
            B = encrypt(QUAD.pack(A) + R[i])
            A = QUAD.unpack(B[:8])[0] ^ (n*j + i)
            R[i] = B[8:]
    return QUAD.pack(A) + b"".join(R[1:])

def aes_wrap_key_withpad(kek, plaintext):
    iv = 0xA65959A600000000 + len(plaintext)
    plaintext = plaintext + b"\0" * ((8 - len(plaintext)) % 8)
    if len(plaintext) == 8:
        return AES.new(kek, AES.MODE_ECB).encrypt(QUAD.pack[iv] + plaintext)
    return aes_wrap_key(kek, plaintext, iv)

def test():
    #test vector from RFC 3394
    import binascii
    KEK = binascii.unhexlify("000102030405060708090A0B0C0D0E0F")
    CIPHER = binascii.unhexlify("1FA68B0A8112B447AEF34BD8FB5A7B829D3E862371D2CFE5")
    PLAIN = binascii.unhexlify("00112233445566778899AABBCCDDEEFF")
    assert aes_unwrap_key(KEK, CIPHER) == PLAIN
    assert aes_wrap_key(KEK, PLAIN) == CIPHER
