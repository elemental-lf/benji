import base64

import pytest
from Crypto.PublicKey import ECC
from benji.config import ConfigDict
from benji.transform.aes_256_gcm_ecc import Transform

CURVE = 'NIST P-384'


def _get_transform(key):
    conf = ConfigDict()
    conf['eccKey'] = base64.b64encode(Transform._pack_envelope_key(key)).decode('ascii')
    conf['eccCurve'] = key.curve
    return Transform(name='EccTest', config=None, module_configuration=conf)


@pytest.fixture
def ecc_transform():
    return _get_transform(ECC.generate(curve=CURVE))


@pytest.fixture
def ecc_transform_2():
    return _get_transform(ECC.generate(curve=CURVE))


@pytest.fixture
def ecc_transform_pubkey_only():
    return _get_transform(ECC.generate(curve=CURVE).public_key())


def test_decryption(ecc_transform):
    data = b'THIS IS A TEST'
    enc_data, materials = ecc_transform.encapsulate(data=data)
    assert enc_data
    assert enc_data != data

    dec_data = ecc_transform.decapsulate(data=enc_data, materials=materials)
    assert dec_data == data


def test_encryption_random(ecc_transform, ecc_transform_2):
    data = b'THIS IS A TEST'

    enc_data, materials = ecc_transform.encapsulate(data=data)
    enc_data_ref, materials_ref = ecc_transform_2.encapsulate(data=data)

    assert enc_data != enc_data_ref
    assert materials['envelope_key'] != materials_ref['envelope_key']


def test_envelope_key(ecc_transform):
    data = b'THIS IS A TEST'

    enc_data, materials = ecc_transform.encapsulate(data=data)

    envelope_key = ecc_transform._unpack_envelope_key(base64.b64decode(materials['envelope_key']))
    assert not envelope_key.has_private()


def test_pubkey_only_encryption(ecc_transform_pubkey_only):
    data = b'THIS IS A TEST'
    enc_data, materials = ecc_transform_pubkey_only.encapsulate(data=data)

    assert enc_data
    assert enc_data != data


def test_pubkey_only_decryption(ecc_transform_pubkey_only):
    data = b'THIS IS A TEST'
    enc_data, materials = ecc_transform_pubkey_only.encapsulate(data=data)
    with pytest.raises(ValueError):
        ecc_transform_pubkey_only.decapsulate(data=enc_data, materials=materials)
