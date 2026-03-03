from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, ExtensionOID, NameOID
import datetime
import ipaddress


def generate_private_key(key_size=4096):
    return rsa.generate_private_key(public_exponent=65537, key_size=key_size)


def pem_encode_pkcs8(private_key, password=None):
    if password is None:
        enc_alg = serialization.NoEncryption()
    else:
        if not isinstance(password, bytes):
            password = password.encode()
        enc_alg = serialization.BestAvailableEncryption(password)
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=enc_alg,
    )


def certificate_builder(
    issuer_name, subject_name, public_key, *, serial_number=None, not_valid_before=None, not_valid_after=None
):
    if not_valid_before is None:
        not_valid_before = datetime.datetime.utcnow()
    # Do not generate certs valid for more than a year
    max_not_valid_after = not_valid_before.replace(year=not_valid_before.year + 1)
    if not_valid_after is None:
        not_valid_after = max_not_valid_after
    elif not_valid_after > max_not_valid_after:
        raise ValueError(f"Cannot be valid after {max_not_valid_after}")
    if not_valid_before >= not_valid_after:
        raise ValueError(f"Invalid validity period: {not_valid_before} to {not_valid_after}")
    if serial_number is None:
        serial_number = x509.random_serial_number()
    return x509.CertificateBuilder(
        issuer_name=issuer_name,
        subject_name=subject_name,
        public_key=public_key,
        serial_number=serial_number,
        not_valid_before=not_valid_before,
        not_valid_after=not_valid_after,
    )


def add_ca_key_usage_extensions(builder):
    return builder.add_extension(
        x509.KeyUsage(
            digital_signature=True,
            content_commitment=True,
            key_encipherment=True,
            data_encipherment=True,
            key_agreement=True,
            key_cert_sign=True,
            crl_sign=True,
            encipher_only=False,
            decipher_only=False,
        ),
        critical=True,
    ).add_extension(
        x509.BasicConstraints(ca=True, path_length=0),
        critical=True,
    )


def add_server_key_usage_extensions(builder):
    return (
        builder.add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [
                    ExtendedKeyUsageOID.SERVER_AUTH,
                    ExtendedKeyUsageOID.CLIENT_AUTH,
                ]
            ),
            critical=False,
        )
    )


def sign(builder, private_key, algorithm=None):
    if algorithm is None:
        algorithm = hashes.SHA512()
    return builder.sign(private_key, algorithm)


def pem_encode(x509_obj):
    return x509_obj.public_bytes(
        encoding=serialization.Encoding.PEM,
    )


def get_common_name(name):
    """Returns common name string from x509.Name"""
    # Get subject common name
    common_name = name.get_attributes_for_oid(NameOID.COMMON_NAME)
    if not common_name:
        raise ValueError("Missing common name attribute")
    if len(common_name) > 1:
        raise ValueError("Contains more than one common name attribute")
    return str(common_name[0].value)


def check_subject_alternative_name(csr):
    san_ext = csr.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
    if san_ext is None:
        raise ValueError("CSR missing Subject Alternative Name extension")
    san = san_ext.value
    # Require the common name to be specified as SAN
    common_name = get_common_name(csr.subject)
    if common_name not in set(map(str, san.get_values_for_type(x509.GeneralName))):
        raise ValueError("Subject common name attribute is not a subject alternative name")
    # TODO: Validate other SAN entries?
    return san


class CertificateAuthority:

    def __init__(self, private_key, certificate):
        self.private_key = private_key
        self.certificate = certificate

    @classmethod
    def generate(cls, name=None):
        if name is None:
            name = x509.Name(
                [
                    x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Hewlett Packard Enterprise Development LP"),
                    x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Dragon"),
                    x509.NameAttribute(NameOID.COMMON_NAME, "Dragon Root CA"),
                ]
            )
        private_key = generate_private_key()
        public_key = private_key.public_key()
        builder = certificate_builder(
            issuer_name=name,
            subject_name=name,
            public_key=public_key,
        )
        builder = (
            add_ca_key_usage_extensions(builder)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(public_key), critical=False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(public_key), critical=False)
        )
        cert = sign(builder, private_key)
        return cls(private_key, cert)

    def issue_server_certificate(self, csr, *, valid_for: datetime.timedelta = None):
        not_valid_before = datetime.datetime.utcnow()
        if valid_for is None:
            # one year
            not_valid_after = not_valid_before.replace(year=not_valid_before.year + 1)
        else:
            not_valid_after = not_valid_before + valid_for
        if not csr.is_signature_valid:
            raise ValueError(f"CSR signature invalid {csr}")
        san = check_subject_alternative_name(csr)
        public_key = csr.public_key()
        builder = certificate_builder(
            issuer_name=self.certificate.subject,
            subject_name=csr.subject,
            public_key=public_key,
            not_valid_before=not_valid_before,
            not_valid_after=not_valid_after,
        )
        builder = (
            add_server_key_usage_extensions(builder)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(public_key), critical=False)
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(self.certificate.public_key()), critical=False
            )
            .add_extension(san, critical=False)
        )
        return sign(builder, self.private_key)


def server_csr_builder(subject_alternative_name):
    # Get subject from the first SAN
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, str(subject_alternative_name[0].value)),
        ]
    )
    return (
        x509.CertificateSigningRequestBuilder()
        .subject_name(subject)
        .add_extension(
            subject_alternative_name,
            critical=False,
        )
    )


def generate_server_csr(ipaddr, *altnames):
    names = [x509.IPAddress(ipaddress.ip_address(ipaddr))]
    if altnames:
        names.extend(altnames)
    private_key = generate_private_key()
    builder = server_csr_builder(x509.SubjectAlternativeName(names))
    csr = sign(builder, private_key)
    return private_key, csr


def generate_server_self_signed_cert(ipaddr, *altnames, valid_for: datetime.timedelta = None):
    names = [x509.IPAddress(ipaddress.ip_address(ipaddr))]
    if altnames:
        names.extend(altnames)
    not_valid_before = datetime.datetime.utcnow()
    if valid_for is None:
        # one year
        not_valid_after = not_valid_before.replace(year=not_valid_before.year + 1)
    else:
        not_valid_after = not_valid_before + valid_for
    private_key = generate_private_key()
    public_key = private_key.public_key()
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, str(names[0].value)),
        ]
    )
    builder = certificate_builder(
        issuer_name=subject,
        subject_name=subject,
        public_key=public_key,
        not_valid_before=not_valid_before,
        not_valid_after=not_valid_after,
    )
    builder = (
        add_server_key_usage_extensions(builder)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(public_key), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(public_key), critical=False)
        .add_extension(x509.SubjectAlternativeName(names), critical=False)
    )
    cert = sign(builder, private_key)
    return private_key, cert


def dragon_uri(rank, host, port):
    return x509.UniformResourceIdentifier(f"urn:dragon:{rank}:{host}:{port}")


if __name__ == "__main__":
    from tempfile import TemporaryDirectory
    import os

    with TemporaryDirectory() as workdir:

        def writefile(path, contents):
            with open(os.path.join(workdir, path), "wb") as f:
                f.write(contents)

        def openssl_x509_text(path):
            os.system(f"openssl x509 -noout -text -in {workdir}/{path}")

        # Generate CA
        ca = CertificateAuthority.generate()

        writefile("ca-key.pem", pem_encode_pkcs8(ca.private_key))
        writefile("ca-cert.pem", pem_encode(ca.certificate))

        openssl_x509_text("ca-cert.pem")

        cluster_nodes = [
            ("127.0.0.1", 7575),
            ("127.0.0.1", 7576),
        ]

        for rank, (host, port) in enumerate(cluster_nodes):
            uri = dragon_uri(rank, host, port)
            key, csr = generate_server_csr(host, uri)
            cert = ca.issue_server_certificate(csr)

            writefile(f"key-{rank}.pem", pem_encode_pkcs8(key))
            writefile(f"cert-{rank}.pem", pem_encode(cert))

            openssl_x509_text(f"cert-{rank}.pem")
