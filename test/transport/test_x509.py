import unittest

from dragon.transport.x509 import (
    CertificateAuthority,
    dragon_uri,
    generate_server_csr,
    generate_server_self_signed_cert,
    pem_encode,
    pem_encode_pkcs8,
)


class X509TestCase(unittest.TestCase):

    @unittest.skip
    def test_generate_private_key(self):
        raise NotImplementedError

    @unittest.skip
    def test_pem_encode_pkcs8(self):
        raise NotImplementedError

    @unittest.skip
    def test_certificate_builder(self):
        raise NotImplementedError

    @unittest.skip
    def test_add_ca_key_usage_extensions(self):
        raise NotImplementedError

    @unittest.skip
    def test_add_server_key_usage_extensions(self):
        raise NotImplementedError

    @unittest.skip
    def test_sign(self):
        raise NotImplementedError

    @unittest.skip
    def test_pem_encode(self):
        raise NotImplementedError

    @unittest.skip
    def test_get_common_name(self):
        raise NotImplementedError

    @unittest.skip
    def test_check_subject_alternative_name(self):
        raise NotImplementedError

    @unittest.skip
    def test_server_csr_builder(self):
        raise NotImplementedError

    @unittest.skip
    def test_generate_server_csr(self):
        raise NotImplementedError

    @unittest.skip
    def test_generate_server_self_signed_cert(self):
        raise NotImplementedError

    @unittest.skip
    def test_dragon_uri(self):
        raise NotImplementedError


class CATestCase(unittest.TestCase):

    @unittest.skip
    def test_generate(self):
        raise NotImplementedError

    @unittest.skip
    def test_issue_server_certificate(self):
        raise NotImplementedError


if __name__ == "__main__":
    unittest.main()
